//! Real worker-cgroup and gang-exec driver for the NixOS action-network VM.
#[cfg(target_os = "linux")]
mod linux {
    use std::error::Error;
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;
    use tokio::process::{Child, Command};

    use gang_main::plugins::Plugins;
    use gang_spi::{ActionLimitsPort, Plugin, PluginContext};
    use gang_types::Bytes;

    type Result<T, E = Box<dyn Error>> = std::result::Result<T, E>;

    fn start(root: &Path, name: &str, network: bool, mode: &str, address: &str) -> Result<Child> {
        let work = root.join("work").join(name).join("work");
        fs::create_dir_all(&work)?;
        let mut command = Command::new(root.join("gang-exec"));
        command
            .current_dir(&work)
            .arg("./probe")
            .arg(mode)
            .arg(address)
            .env_clear()
            .env("GANG_MEMORY_KB", "65536")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        if network {
            command.env("GANG_NETWORK", "requested");
        }
        command.kill_on_drop(true);
        fs::copy(std::env::var("PROBE")?, work.join("probe"))?;
        Ok(command.spawn()?)
    }

    async fn wait(mut child: Child, expected: bool) -> Result<()> {
        let status = tokio::time::timeout(Duration::from_secs(30), child.wait()).await??;
        let code = if expected { 0 } else { 1 };
        if status.code() != Some(code) {
            return Err(format!("action exited {status}, expected code {code}").into());
        }
        Ok(())
    }

    async fn signal(listener: &UnixListener) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(60), async {
            let (mut stream, _) = listener.accept().await?;
            stream.read_exact(&mut [0]).await?;
            Ok::<(), std::io::Error>(())
        })
        .await??;
        Ok(())
    }

    pub async fn run() -> Result<()> {
        let root = PathBuf::from("/var/lib/gang-home/network-test");
        if root.exists() {
            fs::remove_dir_all(&root)?;
        }
        fs::create_dir_all(root.join("work"))?;
        for name in ["gang-exec", "gang-join", "gang-guard"] {
            let path = root.join(name);
            if path.exists() {
                fs::remove_file(&path)?;
            }
            symlink(std::env::var("GANG")?, path)?;
        }
        let config = gang_cgroup::CgroupConfig::default();
        let config_path = root.join("sandbox.toml");
        let cgroup = gang_cgroup::Cgroup::build(
            config,
            &PluginContext {
                config_path: &config_path,
            },
        )?;
        cgroup
            .prepare(Bytes::new(128 * 1024 * 1024))
            .await?
            .ok_or("missing action pool")?;
        let text = format!(
            "work_directory = \"{}\"\nselected = \"bwrap\"\ncontroller_endpoints = [\"http://127.0.0.1:50051\", \"grpc://127.0.0.1:9090\"]\n[actions]\ngrants = [\"network\"]\n[sandbox.bwrap]\nroot = \"{}\"\nprogram = \"{}\"\n[sandbox.bwrap.egress]\nprogram = \"{}\"\n[execution.egress]\nprotected_networks = [\"10.0.0.0/8\", \"11.0.0.1/32\", \"169.254.0.0/16\"]\ndns_servers = [\"1.1.1.1\"]\n[limits.cgroup]\n",
            root.join("work").display(),
            std::env::var("ROOTFS")?,
            std::env::var("BWRAP")?,
            std::env::var("PASTA")?
        );
        let _: gang_config::SandboxFile<Plugins> = text.parse()?;
        fs::write(&config_path, &text)?;
        for pass in 0..2 {
            for (name, network, address, expected) in [
                ("public-granted", true, "11.0.0.2", true),
                ("public-offline", false, "11.0.0.2", false),
                ("imds-granted", true, "169.254.169.254", false),
                ("imds-offline", false, "169.254.169.254", false),
            ] {
                let name = format!("{name}-{pass}");
                wait(start(&root, &name, network, "connect", address)?, expected).await?;
                if !root.join("work").join(&name).join("work/started").exists() {
                    return Err(format!("{name}: payload did not execute").into());
                }
            }
            if pass == 0 {
                let listener = UnixListener::bind(root.join("reload.sock"))?;
                fs::write(root.join("first-done"), b"done")?;
                signal(&listener).await?;
            }
        }
        let dirs =
            ["parallel-a", "parallel-b"].map(|name| root.join("work").join(name).join("work"));
        for dir in &dirs {
            fs::create_dir_all(dir)?;
        }
        let first = UnixListener::bind(dirs[0].join("control.sock"))?;
        let second = UnixListener::bind(dirs[1].join("control.sock"))?;
        let a = start(&root, "parallel-a", true, "barrier", "127.0.0.1")?;
        let b = start(&root, "parallel-b", true, "barrier", "127.0.0.1")?;
        let ((mut a_control, _), (mut b_control, _)) =
            tokio::time::timeout(Duration::from_secs(30), async {
                tokio::try_join!(first.accept(), second.accept())
            })
            .await??;
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut a_ready = [0];
            let mut b_ready = [0];
            tokio::try_join!(
                a_control.read_exact(&mut a_ready),
                b_control.read_exact(&mut b_ready)
            )?;
            a_control.write_all(b"x").await?;
            b_control.write_all(b"x").await
        })
        .await??;
        wait(a, true).await?;
        wait(b, true).await?;
        fs::write(&config_path, text.replace("[limits.cgroup]\n", ""))?;
        wait(
            start(&root, "no-scope", true, "connect", "11.0.0.2")?,
            false,
        )
        .await?;
        if root.join("work/no-scope/work/started").exists() {
            return Err("no-scope payload executed".into());
        }
        fs::write(&config_path, text.replace("1.1.1.1", "8.8.8.8"))?;
        wait(
            start(&root, "mismatch", true, "connect", "11.0.0.2")?,
            false,
        )
        .await?;
        if root.join("work/mismatch/work/started").exists() {
            return Err("mismatched-policy payload executed".into());
        }
        fs::write(&config_path, &text)?;
        let listener = UnixListener::bind(root.join("stamp.sock"))?;
        fs::write(root.join("remove-stamp"), b"ready")?;
        signal(&listener).await?;
        wait(
            start(&root, "missing-stamp", true, "connect", "11.0.0.2")?,
            false,
        )
        .await?;
        if root.join("work/missing-stamp/work/started").exists() {
            return Err("missing-stamp payload executed".into());
        }
        wait(
            start(&root, "offline-no-stamp", false, "connect", "11.0.0.2")?,
            false,
        )
        .await?;
        if !root.join("work/offline-no-stamp/work/started").exists() {
            return Err("offline payload did not execute without stamp".into());
        }
        fs::write(root.join("all-done"), b"done")?;
        Ok(())
    }
}

/// Runs the Linux action-network VM fixture.
///
/// # Errors
///
/// Returns a diagnostic if the fixture fails or the platform is unsupported.
pub fn run_action_network() -> std::result::Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        runtime
            .block_on(linux::run())
            .map_err(|error| error.to_string())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("action network requires Linux".into())
    }
}
