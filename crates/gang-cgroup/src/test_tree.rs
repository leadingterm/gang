//! Fake cgroup v2 tree on a temporary directory for the controller tests.
//!
//! Models the kernel rules the plugin must respect: a cgroup holding processes
//! cannot enable controllers for its children, a controller can only be enabled
//! when the parent has it available, controller interface files exist only while
//! the parent delegates the controller, and removal fails while processes or
//! child cgroups remain. `cgroup.events` is computed on every read: it reports
//! `populated 1` while processes remain (each read lets one exiting process go)
//! or while the modelled process is inside the cgroup. Delegation, process
//! start times and missing kernel files are configured through the builders.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use tempfile::TempDir;

use crate::fs::{
    CONTROLLERS, CPU_MAX, CPU_STAT, CPU_WEIGHT, Cgroupfs, EVENTS, KILL, MEMORY_EVENTS, MEMORY_MAX,
    MEMORY_OOM_GROUP, MEMORY_PEAK, MEMORY_SWAP_MAX, PIDS_MAX, PROCS, SUBTREE_CONTROL,
    child_directories,
};

const MEMORY: &str = "memory";
const CPU: &str = "cpu";
const PIDS: &str = "pids";

/// Start time the controller's own process has unless a test changes it.
pub(crate) const OWN_START: u64 = 1_000;

/// Interface files that exist only while the parent delegates their controller.
const GATED_FILES: [(&str, &str); 6] = [
    (MEMORY_MAX, MEMORY),
    (MEMORY_SWAP_MAX, MEMORY),
    (MEMORY_OOM_GROUP, MEMORY),
    (CPU_MAX, CPU),
    (CPU_WEIGHT, CPU),
    (PIDS_MAX, PIDS),
];

#[derive(Default)]
struct State {
    /// The cgroup the modelled process lives in.
    member: PathBuf,
    /// Physical memory in KiB, or the default test-machine size.
    memtotal_kib: Option<u64>,
    /// Page size used to emulate kernel memory.max rounding.
    page_size: Option<u64>,
    /// Writes before kernel page rounding.
    memory_max_writes: Vec<(PathBuf, String)>,
    /// Value seeded into `memory.peak` of newly created cgroups.
    peak_memory: u64,
    /// Value seeded into `cpu.stat` `usage_usec` of newly created cgroups.
    cpu_micros: u64,
    /// Text seeded into `memory.events`.
    memory_events: String,
    /// Failed removals a cgroup is charged with after `cgroup.kill`.
    deaths_after_kill: u32,
    /// Files whose every write fails with `PermissionDenied`.
    rejected_writes: BTreeSet<String>,
    /// Interface files the kernel does not provide: newly created cgroups are not seeded with
    /// them, and gated ones are absent.
    unseeded: BTreeSet<String>,
    /// Whether the subtree is withheld from the worker.
    undelegated: bool,
    /// Start time per process, as [`Cgroupfs::process_start`] reports it. The controller's own
    /// process is present unless a test removes it.
    starts: BTreeMap<u32, u64>,
    /// Processes that still have a stat entry but have exited as zombies.
    zombies: BTreeSet<u32>,
    /// Cgroups that received a `cgroup.kill` write.
    killed: BTreeSet<PathBuf>,
    /// Cgroups another reaper removes just before this process writes `cgroup.kill` to them.
    raced_removals: BTreeSet<PathBuf>,
    /// Remaining processes per cgroup, each read of `cgroup.events` lets one exit.
    busy: HashMap<PathBuf, u32>,
    /// Controllers enabled for the children of each cgroup.
    enabled: BTreeMap<PathBuf, BTreeSet<String>>,
}

impl State {
    fn enabled_in(&self, cgroup: &Path) -> BTreeSet<String> {
        self.enabled.get(cgroup).cloned().unwrap_or_default()
    }

    /// Whether the gated interface file `file` of `controller` is present in `cgroup`: the kernel
    /// exposes it in every child of a parent that delegates the controller, unless withheld.
    fn gated_present(&self, cgroup: &Path, controller: &str, file: &str) -> bool {
        self.enabled
            .get(parent_of(cgroup))
            .is_some_and(|controllers| controllers.contains(controller))
            && !self.unseeded.contains(file)
    }
}

/// A cgroup tree rooted in a temporary directory, implementing [`Cgroupfs`].
pub(crate) struct TestTree {
    root: TempDir,
    state: Mutex<State>,
}

impl TestTree {
    /// A tree whose modelled process lives in `subtree`, which delegates `controllers`.
    pub(crate) fn new(subtree: &str, controllers: &[&str]) -> Self {
        let root = TempDir::new().expect("create temp cgroup root");
        let tree = Self {
            root,
            state: Mutex::new(State::default()),
        };
        let dir = tree.absolute(Path::new(subtree));
        fs::create_dir_all(&dir).expect("create delegated subtree");
        fs::write(dir.join(CONTROLLERS), controllers.join(" ")).expect("seed controllers");
        fs::write(dir.join(SUBTREE_CONTROL), "").expect("seed subtree_control");
        for ancestor in Path::new(subtree)
            .ancestors()
            .filter(|path| !path.as_os_str().is_empty())
        {
            fs::write(tree.absolute(ancestor).join(MEMORY_MAX), "max\n")
                .expect("seed ancestor memory.max");
        }
        tree.state.lock().member = PathBuf::from(subtree);
        tree.state
            .lock()
            .starts
            .insert(std::process::id(), OWN_START);
        tree
    }

    /// Configures the physical RAM returned by the procfs seam, in KiB.
    pub(crate) fn with_memtotal_kib(self, kib: u64) -> Self {
        self.state.lock().memtotal_kib = Some(kib);
        self
    }

    /// Configures the page size used for `memory.max` read-back.
    pub(crate) fn with_page_size(self, bytes: u64) -> Self {
        assert!(bytes > 0, "page size must be nonzero");
        self.state.lock().page_size = Some(bytes);
        self
    }

    /// Sets an ancestor's memory limit text.
    pub(crate) fn set_memory_max(&self, cgroup: &Path, value: &str) {
        fs::write(self.absolute(cgroup).join(MEMORY_MAX), value).expect("seed memory.max");
    }

    /// Returns writes to `memory.max` before page rounding.
    pub(crate) fn memory_max_writes(&self) -> Vec<(PathBuf, String)> {
        self.state.lock().memory_max_writes.clone()
    }

    /// Sets the `memory.peak` value of cgroups created afterwards.
    pub(crate) fn with_peak_memory(self, bytes: u64) -> Self {
        self.state.lock().peak_memory = bytes;
        self
    }

    /// Sets the `cpu.stat` `usage_usec` value of cgroups created afterwards.
    pub(crate) fn with_cpu_micros(self, micros: u64) -> Self {
        self.state.lock().cpu_micros = micros;
        self
    }
    /// Sets the `memory.events` text of cgroups created afterwards.
    pub(crate) fn with_memory_events(self, text: &str) -> Self {
        self.state.lock().memory_events = text.to_owned();
        self
    }

    /// Sets how many failed removals a cgroup is charged with after `cgroup.kill`.
    pub(crate) fn with_deaths_after_kill(self, attempts: u32) -> Self {
        self.state.lock().deaths_after_kill = attempts;
        self
    }

    /// Makes another reaper remove `cgroup`, with its descendants, just before the controller's
    /// `cgroup.kill` write reaches it.
    pub(crate) fn removed_by_a_racing_reaper(self, cgroup: &Path) -> Self {
        self.state
            .lock()
            .raced_removals
            .insert(cgroup.to_path_buf());
        self
    }

    /// Makes every write to `file` fail with `PermissionDenied`.
    pub(crate) fn reject_writes_of(self, file: &str) -> Self {
        self.state.lock().rejected_writes.insert(file.to_owned());
        self
    }

    /// Withholds the delegation of the tree from the worker.
    pub(crate) fn undelegated(self) -> Self {
        self.state.lock().undelegated = true;
        self
    }

    /// Creates cgroups afterwards without the interface file `file`.
    pub(crate) fn without_kernel_file(self, file: &str) -> Self {
        self.state.lock().unseeded.insert(file.to_owned());
        self
    }

    /// Sets the start time [`Cgroupfs::process_start`] reports for `pid`; `None` makes the
    /// process vanish.
    pub(crate) fn set_process_start(&self, pid: u32, start: Option<u64>) {
        let mut state = self.state.lock();
        state.zombies.remove(&pid);
        match start {
            Some(start) => {
                state.starts.insert(pid, start);
            }
            None => {
                state.starts.remove(&pid);
            }
        }
    }

    /// Marks `pid` as a zombie while retaining its modelled start time.
    pub(crate) fn set_zombie(&self, pid: u32) {
        self.state.lock().zombies.insert(pid);
    }

    /// Marks an existing cgroup as populated until `busy_attempts` reads of
    /// `cgroup.events` have consumed its processes; removals fail meanwhile.
    pub(crate) fn populate(&self, cgroup: &Path, busy_attempts: u32) {
        self.state
            .lock()
            .busy
            .insert(cgroup.to_path_buf(), busy_attempts);
    }

    /// The cgroup the modelled process currently lives in.
    pub(crate) fn member(&self) -> PathBuf {
        self.state.lock().member.clone()
    }

    /// Every cgroup that received a `cgroup.kill` write, sorted.
    pub(crate) fn killed(&self) -> Vec<PathBuf> {
        self.state.lock().killed.iter().cloned().collect()
    }

    /// Whether the cgroup directory exists.
    pub(crate) fn exists(&self, cgroup: &Path) -> bool {
        self.absolute(cgroup).is_dir()
    }

    /// The content of an interface file, `None` when it does not exist.
    pub(crate) fn file(&self, cgroup: &Path, name: &str) -> Option<String> {
        fs::read_to_string(self.absolute(cgroup).join(name)).ok()
    }

    /// The controllers enabled for the children of `cgroup`, sorted.
    pub(crate) fn enabled(&self, cgroup: &Path) -> Vec<String> {
        self.state.lock().enabled_in(cgroup).into_iter().collect()
    }

    /// Creates `cgroup` and its missing parents, seeded like [`Cgroupfs::create`].
    pub(crate) fn mkdir_all(&self, cgroup: &Path) {
        let mut current = PathBuf::new();
        for component in cgroup.components() {
            current.push(component);
            if !self.exists(&current) {
                self.create(&current).expect("create cgroup");
            }
        }
    }

    /// The computed `cgroup.events` content of `cgroup`; a read consumes one exiting process.
    fn events(&self, cgroup: &Path) -> io::Result<String> {
        if !self.absolute(cgroup).is_dir() {
            return Err(io::ErrorKind::NotFound.into());
        }
        let mut state = self.state.lock();
        let draining = match state.busy.get_mut(cgroup) {
            Some(attempts) if *attempts > 0 => {
                *attempts -= 1;
                true
            }
            _ => false,
        };
        let populated = draining || state.member.starts_with(cgroup);
        Ok(format!("populated {}\nfrozen 0\n", u8::from(populated)))
    }
}

impl Cgroupfs for TestTree {
    fn own_cgroup(&self) -> io::Result<PathBuf> {
        Ok(self.state.lock().member.clone())
    }

    fn meminfo(&self) -> io::Result<String> {
        Ok(format!(
            "MemTotal: {} kB\n",
            self.state.lock().memtotal_kib.unwrap_or(8_388_608)
        ))
    }

    fn absolute(&self, cgroup: &Path) -> PathBuf {
        self.root.path().join(cgroup)
    }

    fn has_file(&self, cgroup: &Path, file: &str) -> bool {
        let state = self.state.lock();
        if let Some(controller) = gate(file) {
            return state.gated_present(cgroup, controller, file);
        }
        self.absolute(cgroup).join(file).exists()
    }

    fn delegated(&self, _cgroup: &Path) -> io::Result<bool> {
        Ok(!self.state.lock().undelegated)
    }

    fn process_start(&self, pid: u32) -> Option<u64> {
        let state = self.state.lock();
        if state.zombies.contains(&pid) {
            None
        } else {
            state.starts.get(&pid).copied()
        }
    }

    fn create(&self, cgroup: &Path) -> io::Result<()> {
        let dir = self.absolute(cgroup);
        fs::create_dir(&dir)?;
        let state = self.state.lock();
        let controllers = state.enabled_in(parent_of(cgroup));
        seed(&dir, &state, &controllers)
    }

    fn remove(&self, cgroup: &Path) -> io::Result<()> {
        if !self.children(cgroup)?.is_empty() {
            return Err(busy());
        }
        let mut state = self.state.lock();
        if state.busy.get(cgroup).is_some_and(|attempts| *attempts > 0) {
            return Err(busy());
        }
        state.busy.remove(cgroup);
        state.enabled.remove(cgroup);
        drop(state);
        fs::remove_dir_all(self.absolute(cgroup))
    }

    fn read(&self, cgroup: &Path, file: &str) -> io::Result<String> {
        if file == EVENTS {
            return self.events(cgroup);
        }
        fs::read_to_string(self.absolute(cgroup).join(file))
    }

    fn write(&self, cgroup: &Path, file: &str, value: &str) -> io::Result<()> {
        let dir = self.absolute(cgroup);
        let mut state = self.state.lock();
        if state.rejected_writes.contains(file) {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        if file == KILL && state.raced_removals.remove(cgroup) {
            fs::remove_dir_all(&dir)?;
        }
        if !dir.is_dir() {
            return Err(io::ErrorKind::NotFound.into());
        }
        match file {
            PROCS => state.member = cgroup.to_path_buf(),
            SUBTREE_CONTROL => write_subtree_control(&dir, cgroup, &mut state, value)?,
            KILL => {
                let attempts = state.deaths_after_kill;
                state.killed.insert(cgroup.to_path_buf());
                let busy = state.busy.entry(cgroup.to_path_buf()).or_insert(0);
                *busy = (*busy).max(attempts);
            }
            _ => {
                if let Some(controller) = gate(file)
                    && !state.gated_present(cgroup, controller, file)
                {
                    return Err(io::ErrorKind::NotFound.into());
                }
                if file == MEMORY_MAX {
                    state
                        .memory_max_writes
                        .push((cgroup.to_path_buf(), value.to_owned()));
                    let page = state.page_size.unwrap_or(1);
                    let parsed = value
                        .parse::<u64>()
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    let rounded = parsed
                        .checked_add(page - 1)
                        .ok_or(io::ErrorKind::InvalidData)?
                        / page
                        * page;
                    fs::write(dir.join(file), rounded.to_string())?;
                } else {
                    fs::write(dir.join(file), value)?;
                }
            }
        }
        Ok(())
    }

    fn children(&self, cgroup: &Path) -> io::Result<Vec<PathBuf>> {
        child_directories(&self.absolute(cgroup), cgroup)
    }
}

/// Applies a `cgroup.subtree_control` write atomically: any bad token leaves the state untouched.
fn write_subtree_control(
    dir: &Path,
    cgroup: &Path,
    state: &mut State,
    value: &str,
) -> io::Result<()> {
    if state.member.as_path() == cgroup {
        return Err(busy());
    }
    let parent = parent_of(cgroup);
    let available: BTreeSet<String> = if state.enabled.contains_key(parent) {
        state.enabled_in(parent)
    } else {
        fs::read_to_string(dir.join(CONTROLLERS))?
            .split_whitespace()
            .map(str::to_owned)
            .collect()
    };
    let mut enabled = state.enabled_in(cgroup);
    for token in value.split_whitespace() {
        if let Some(name) = token.strip_prefix('+') {
            if !available.contains(name) {
                return Err(io::ErrorKind::NotFound.into());
            }
            enabled.insert(name.to_owned());
        } else if let Some(name) = token.strip_prefix('-') {
            enabled.remove(name);
        } else {
            return Err(io::ErrorKind::InvalidInput.into());
        }
    }
    fs::write(dir.join(SUBTREE_CONTROL), join(&enabled))?;
    state.enabled.insert(cgroup.to_path_buf(), enabled);
    Ok(())
}

/// Writes the files every new cgroup starts with, minus the ones the tree withholds.
fn seed(dir: &Path, state: &State, controllers: &BTreeSet<String>) -> io::Result<()> {
    let files = [
        (PROCS, String::new()),
        (KILL, String::new()),
        (SUBTREE_CONTROL, String::new()),
        (CONTROLLERS, join(controllers)),
        (MEMORY_MAX, "max\n".to_owned()),
        (MEMORY_PEAK, format!("{}\n", state.peak_memory)),
        (MEMORY_EVENTS, state.memory_events.clone()),
        (
            CPU_STAT,
            format!(
                "usage_usec {}\nuser_usec 0\nsystem_usec 0\n",
                state.cpu_micros
            ),
        ),
    ];
    for (name, content) in files {
        if !state.unseeded.contains(name) {
            fs::write(dir.join(name), content)?;
        }
    }
    Ok(())
}

/// The controller an interface file belongs to, if it is gated.
fn gate(file: &str) -> Option<&'static str> {
    GATED_FILES
        .iter()
        .find(|(name, _)| *name == file)
        .map(|&(_, controller)| controller)
}

fn parent_of(cgroup: &Path) -> &Path {
    cgroup.parent().unwrap_or(Path::new(""))
}

fn busy() -> io::Error {
    io::ErrorKind::ResourceBusy.into()
}

fn join(names: &BTreeSet<String>) -> String {
    names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(" ")
}
