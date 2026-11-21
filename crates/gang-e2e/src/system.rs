//! What a scenario sees of the system under test, as one trait, so that the scenarios run
//! against the real `gang` binary ([`Stack`](crate::Stack)) and against a simulation in tests.

use std::fmt;
use std::path::{Path, PathBuf};

use gang_config::InputLinks;
use gang_types::NonZeroBytes;
use gang_wire::{Event, Frame, State};

use crate::access::IdentityKnobs;
use crate::build_output::BuildOutcome;
use crate::control::ControlKnobs;
use crate::errors::Result;
use crate::execution::{ExecutionKnobs, Run};
use crate::host::Host;
use crate::report::ProbeReport;
use crate::server::ServerKnobs;

/// The sandbox the workers run actions under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sandbox {
    /// Runs the action directly (macOS).
    Noop,
    /// Runs the action under bubblewrap (Linux).
    Bwrap,
}

impl Sandbox {
    /// The sandbox of this platform: `bwrap` on Linux, `noop` elsewhere.
    #[must_use]
    pub const fn native() -> Self {
        if cfg!(target_os = "linux") {
            Self::Bwrap
        } else {
            Self::Noop
        }
    }

    /// The key of its `[sandbox.<key>]` section and its `sandbox` value in a `[[fleets]]` entry.
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::Bwrap => "bwrap",
        }
    }
}

impl fmt::Display for Sandbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.key())
    }
}

/// Whether the fleet grants the network capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermitNetwork {
    /// The fleet does not grant network access.
    Denied,
    /// The fleet grants public egress to requesting actions.
    Permitted,
}

/// Memory of the `memory_kb` platform property of the limit probes, in KiB.
pub const HOLD_MEMORY_KB: u64 = 65_536;

/// The `cpu_count` platform property of the limit probes.
pub const HOLD_CPU_COUNT: u32 = 1;

/// The `[limits.cgroup] max_pids` the workers are configured with.
pub const LIMIT_MAX_PIDS: u32 = 4096;

/// The `[limits.cgroup] cpu_period` the workers are configured with, in microseconds (`100ms`).
pub const LIMIT_CPU_PERIOD_MICROS: u64 = 100_000;

/// Whether the workers enforce per-action limits (`[limits.cgroup]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionLimits {
    /// The workers set no per-action limits.
    Absent,
    /// The workers run every action in a cgroup with CPU and pid limits and group OOM handling.
    Cgroup,
}

/// Whether opt-in soft memory enforcement is enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoftMemory {
    /// Kill an action over its soft ceiling and requeue it.
    Enforced,
    /// Soft enforcement is disabled.
    Off,
}

/// The first fleet of [`FleetLayout::Placement`]: the base fleet's size, one instance at most.
pub const SMALL_FLEET: &str = "local-small";

/// The second fleet of [`FleetLayout::Placement`]: twice the size of [`SMALL_FLEET`], the same
/// sandbox, two instances at most.
pub const LARGE_FLEET: &str = "local-large";

/// The per-action memory ceiling of the small and noop placement fleets.
pub const PLACEMENT_SMALL_MEMORY: NonZeroBytes = NonZeroBytes::gib(1);

/// The per-action memory ceiling of the large placement fleet and its memory probe.
pub const PLACEMENT_LARGE_MEMORY: NonZeroBytes = NonZeroBytes::gib(2);

/// The third fleet of [`FleetLayout::Placement`]: the size of [`SMALL_FLEET`], the noop sandbox,
/// one instance at most.
pub const NOOP_FLEET: &str = "local-noop";

/// The `[[fleets]]` a deployment declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetLayout {
    /// The one fleet of the base configuration.
    Single,
    /// Three fleets in preference order: [`SMALL_FLEET`], [`LARGE_FLEET`] and [`NOOP_FLEET`].
    Placement,
}

/// The configuration knobs the scenarios vary; everything else comes from the base `gang.toml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Variant {
    /// The sandbox of the workers.
    pub sandbox: Sandbox,
    /// Whether the fleet grants the network capability.
    pub permit_network: PermitNetwork,
    /// `[execution] input_links`.
    pub input_links: InputLinks,
    /// Whether the workers enforce per-action limits (`[limits.cgroup]`).
    pub limits: ActionLimits,
    /// The `[[fleets]]` the deployment declares.
    pub fleets: FleetLayout,
    /// Whether `NativeLink`'s soft memory enforcement runs beside the kernel limit.
    pub soft_memory: SoftMemory,
    /// Whether the disk scenario opts into `NativeLink`'s soft disk enforcement.
    pub soft_disk: bool,
    /// The identity family's knobs.
    pub identity: IdentityKnobs,
    /// The control-loop keys of the control scenarios.
    pub control: ControlKnobs,
    /// The server family's `[server]`, `[server.rate_limit]` and `[dashboard]` knobs.
    pub server: ServerKnobs,
    /// The knobs of the execution scenarios.
    pub execution: ExecutionKnobs,
}

impl Variant {
    /// The configuration most scenarios run under: no network permission, input links broken.
    #[must_use]
    pub const fn baseline(sandbox: Sandbox) -> Self {
        Self {
            sandbox,
            permit_network: PermitNetwork::Denied,
            input_links: InputLinks::Break,
            limits: ActionLimits::Absent,
            fleets: FleetLayout::Single,
            soft_memory: SoftMemory::Off,
            soft_disk: false,
            identity: IdentityKnobs::NONE,
            control: ControlKnobs::NONE,
            server: ServerKnobs::NONE,
            execution: ExecutionKnobs::NONE,
        }
    }
}

gang_types::string_enum! {
    /// A target of the sample workspace, each an action that reports what it observed.
    pub enum Probe("probe") {
        /// Records the net and mnt namespaces, the visibility of a stand-in for the CAS, and whether
        /// its working directory is writable.
        Isolated = "isolated",
        /// Probes network access with `network = requested`.
        NetworkRequested = "network_requested",
        /// Records that it ran; sets the `ISA` exec property explicitly.
        Pinned = "pinned",
        /// Copies a large input to a distinct output for cache traffic observation.
        CacheVisibility = "cache_visibility",
        /// Runs `sh -c 'exit 3'`.
        ExitThree = "exit_three",
        /// Tries to modify its cached input in place.
        Poison = "poison",
        /// Runs after [`Probe::Poison`] and records whether the shared input was modified.
        Victim = "victim",
        /// Reads an input of an external repository from beside its working directory, directly
        /// and through a relative symlink, and records the input's link count.
        InputRoot = "input_root",
        /// Runs in its own cgroup scope and waits until released, leaving an orphan process behind.
        Hold = "hold",
        /// Allocates above its reservation on a healthy worker.
        MemoryHog = "memory_hog",
        /// Holds a plateau above the explicitly enabled soft limit until killed and requeued.
        MemoryGrower = "memory_grower",
        /// Holds the action selected as the memory-pressure victim.
        PressureVictim = "pressure_victim",
        /// Holds a larger resident allocation while a neighbour experiences pressure.
        PressureNeighbour = "pressure_neighbour",
        /// Writes 64 MiB under its working directory, past its disk reservation, and waits to be
        /// killed by the worker.
        DiskHog = "disk_hog",
        /// Writes 7.5 MiB in steps while the disk hog runs, then reports whether its file is intact.
        DiskNeighbour = "disk_neighbour",
        /// Records the net and mnt namespaces; sets no exec property but the ISA.
        Unconstrained = "fleet_unconstrained",
        /// Records the net and mnt namespaces; requests `fleet = local-large`.
        FleetRequested = "fleet_requested",
        /// Records the net and mnt namespaces; requests `memory_kb` above the first fleet's size.
        MemoryRequested = "memory_requested",
        /// Records the net and mnt namespaces; requests `sandbox = noop`.
        SandboxRequested = "sandbox_requested",
        /// Records that it ran; sets no exec property but the ISA. Is built beside
        /// [`Probe::OverflowSecond`].
        OverflowFirst = "overflow_first",
        /// Records that it ran; sets no exec property but the ISA. Is built beside
        /// [`Probe::OverflowFirst`].
        OverflowSecond = "overflow_second",
        /// Outgrows the soft limit of every reservation it is given and holds, so every attempt
        /// is killed; appends one line per attempt to the attempts file.
        MemoryEscalation = "memory_escalation",
        /// Reserves 512 TiB of disk, more than any host has free.
        DiskGuarded = "disk_guarded",
        /// Starts a background child that ignores SIGTERM, then waits forever.
        TimeoutChild = "timeout_child",
        /// Waits forever; on SIGTERM it reports to the journal and exits 0 a second later.
        TermTrap = "term_trap",
        /// Waits forever and keeps running after SIGTERM.
        TermIgnorer = "term_ignorer",
        /// Sends itself SIGKILL.
        SelfKill = "self_kill",
        /// Sends itself SIGTERM.
        SelfTerm = "self_term",
        /// Records its environment's variable names, its /tmp, whether each mount point is
        /// writable, and whether its working directory is writable.
        SandboxContract = "sandbox_contract",
        /// Runs the staged dynamically linked glibc program and records whether it started.
        GlibcTool = "glibc_tool",
        /// Runs a `#!/usr/bin/env bash` script with Bazel's strict action `PATH` and records whether it ran.
        FhsTools = "fhs_tools",
        /// Leaves a process in its own process group behind and waits for the go flag.
        Stray = "stray",
        /// Reports to the journal and waits for the go flag.
        HeldRelease = "held_release",
        /// First held controller-loss action.
        ControllerLoss0 = "controller_loss_0",
        /// Second held controller-loss action.
        ControllerLoss1 = "controller_loss_1",
        /// Third held controller-loss action.
        ControllerLoss2 = "controller_loss_2",
        /// Fourth held controller-loss action.
        ControllerLoss3 = "controller_loss_3",
        /// Fifth held controller-loss action.
        ControllerLoss4 = "controller_loss_4",
        /// Sixth held controller-loss action.
        ControllerLoss5 = "controller_loss_5",
        /// Seventh held controller-loss action.
        ControllerLoss6 = "controller_loss_6",
        /// Eighth held controller-loss action.
        ControllerLoss7 = "controller_loss_7",
        /// First held database-partition action, with independent duration history.
        Partition0 = "partition_0",
        /// Second held database-partition action.
        Partition1 = "partition_1",
        /// Third held database-partition action.
        Partition2 = "partition_2",
        /// Fourth held database-partition action.
        Partition3 = "partition_3",
        /// Fifth held database-partition action.
        Partition4 = "partition_4",
        /// Sixth held database-partition action.
        Partition5 = "partition_5",
        /// Seventh held database-partition action.
        Partition6 = "partition_6",
        /// Eighth held database-partition action.
        Partition7 = "partition_7",
        /// Source A's first refilling lane.
        RateA0 = "rate_a_0_63",
        /// Source A's second refilling lane.
        RateA1 = "rate_a_1_63",
        /// Source A's third refilling lane.
        RateA2 = "rate_a_2_63",
        /// Source B's first refilling lane.
        RateB0 = "rate_b_0_63",
        /// Source B's second refilling lane.
        RateB1 = "rate_b_1_63",
        /// Source B's third refilling lane.
        RateB2 = "rate_b_2_63",
        /// Source B's fourth refilling lane.
        RateB3 = "rate_b_3_63",
        /// Source B's fifth refilling lane.
        RateB4 = "rate_b_4_63",
        /// Held utilization probe 00.
        RateHold00 = "rate_hold_00",
        /// Held utilization probe 01.
        RateHold01 = "rate_hold_01",
        /// Held utilization probe 02.
        RateHold02 = "rate_hold_02",
        /// Held utilization probe 03.
        RateHold03 = "rate_hold_03",
        /// Held utilization probe 04.
        RateHold04 = "rate_hold_04",
        /// Held utilization probe 05.
        RateHold05 = "rate_hold_05",
        /// Held utilization probe 06.
        RateHold06 = "rate_hold_06",
        /// Held utilization probe 07.
        RateHold07 = "rate_hold_07",
        /// Held utilization probe 08.
        RateHold08 = "rate_hold_08",
        /// Held utilization probe 09.
        RateHold09 = "rate_hold_09",
        /// Held utilization probe 10.
        RateHold10 = "rate_hold_10",
        /// Held utilization probe 11.
        RateHold11 = "rate_hold_11",
        /// Held utilization probe 12.
        RateHold12 = "rate_hold_12",
        /// Held utilization probe 13.
        RateHold13 = "rate_hold_13",
        /// Held utilization probe 14.
        RateHold14 = "rate_hold_14",
        /// Held utilization probe 15.
        RateHold15 = "rate_hold_15",
        /// Held utilization probe 16.
        RateHold16 = "rate_hold_16",
        /// Held utilization probe 17.
        RateHold17 = "rate_hold_17",
        /// Held utilization probe 18.
        RateHold18 = "rate_hold_18",
        /// Held utilization probe 19.
        RateHold19 = "rate_hold_19",
        /// Held utilization probe 20.
        RateHold20 = "rate_hold_20",
        /// Held utilization probe 21.
        RateHold21 = "rate_hold_21",
        /// Held utilization probe 22.
        RateHold22 = "rate_hold_22",
        /// Held utilization probe 23.
        RateHold23 = "rate_hold_23",
        /// First action of returning load.
        RateReturn00 = "rate_return_00",
        /// Second action of returning load.
        RateReturn01 = "rate_return_01",
        /// Third action of returning load.
        RateReturn02 = "rate_return_02",
        /// Fourth action of returning load.
        RateReturn03 = "rate_return_03",
        /// Fifth action of returning load.
        RateReturn04 = "rate_return_04",
        /// Sixth action of returning load.
        RateReturn05 = "rate_return_05",
        /// Seventh action of returning load.
        RateReturn06 = "rate_return_06",
        /// Eighth action of returning load.
        RateReturn07 = "rate_return_07",
        /// Short burst probe 000.
        Burst000 = "burst_000",
        /// Short burst probe 001.
        Burst001 = "burst_001",
        /// Short burst probe 002.
        Burst002 = "burst_002",
        /// Short burst probe 003.
        Burst003 = "burst_003",
        /// Short burst probe 004.
        Burst004 = "burst_004",
        /// Short burst probe 005.
        Burst005 = "burst_005",
        /// Short burst probe 006.
        Burst006 = "burst_006",
        /// Short burst probe 007.
        Burst007 = "burst_007",
        /// Short burst probe 008.
        Burst008 = "burst_008",
        /// Short burst probe 009.
        Burst009 = "burst_009",
        /// Short burst probe 010.
        Burst010 = "burst_010",
        /// Short burst probe 011.
        Burst011 = "burst_011",
        /// Short burst probe 012.
        Burst012 = "burst_012",
        /// Short burst probe 013.
        Burst013 = "burst_013",
        /// Short burst probe 014.
        Burst014 = "burst_014",
        /// Short burst probe 015.
        Burst015 = "burst_015",
        /// Short burst probe 016.
        Burst016 = "burst_016",
        /// Short burst probe 017.
        Burst017 = "burst_017",
        /// Short burst probe 018.
        Burst018 = "burst_018",
        /// Short burst probe 019.
        Burst019 = "burst_019",
        /// Short burst probe 020.
        Burst020 = "burst_020",
        /// Short burst probe 021.
        Burst021 = "burst_021",
        /// Short burst probe 022.
        Burst022 = "burst_022",
        /// Short burst probe 023.
        Burst023 = "burst_023",
        /// Short burst probe 024.
        Burst024 = "burst_024",
        /// Short burst probe 025.
        Burst025 = "burst_025",
        /// Short burst probe 026.
        Burst026 = "burst_026",
        /// Short burst probe 027.
        Burst027 = "burst_027",
        /// Short burst probe 028.
        Burst028 = "burst_028",
        /// Short burst probe 029.
        Burst029 = "burst_029",
        /// Short burst probe 030.
        Burst030 = "burst_030",
        /// Short burst probe 031.
        Burst031 = "burst_031",
        /// Short burst probe 032.
        Burst032 = "burst_032",
        /// Short burst probe 033.
        Burst033 = "burst_033",
        /// Short burst probe 034.
        Burst034 = "burst_034",
        /// Short burst probe 035.
        Burst035 = "burst_035",
        /// Short burst probe 036.
        Burst036 = "burst_036",
        /// Short burst probe 037.
        Burst037 = "burst_037",
        /// Short burst probe 038.
        Burst038 = "burst_038",
        /// Short burst probe 039.
        Burst039 = "burst_039",
        /// Short burst probe 040.
        Burst040 = "burst_040",
        /// Short burst probe 041.
        Burst041 = "burst_041",
        /// Short burst probe 042.
        Burst042 = "burst_042",
        /// Short burst probe 043.
        Burst043 = "burst_043",
        /// Short burst probe 044.
        Burst044 = "burst_044",
        /// Short burst probe 045.
        Burst045 = "burst_045",
        /// Short burst probe 046.
        Burst046 = "burst_046",
        /// Short burst probe 047.
        Burst047 = "burst_047",
        /// Short burst probe 048.
        Burst048 = "burst_048",
        /// Short burst probe 049.
        Burst049 = "burst_049",
        /// Short burst probe 050.
        Burst050 = "burst_050",
        /// Short burst probe 051.
        Burst051 = "burst_051",
        /// Short burst probe 052.
        Burst052 = "burst_052",
        /// Short burst probe 053.
        Burst053 = "burst_053",
        /// Short burst probe 054.
        Burst054 = "burst_054",
        /// Short burst probe 055.
        Burst055 = "burst_055",
        /// Short burst probe 056.
        Burst056 = "burst_056",
        /// Short burst probe 057.
        Burst057 = "burst_057",
        /// Short burst probe 058.
        Burst058 = "burst_058",
        /// Short burst probe 059.
        Burst059 = "burst_059",
        /// Short burst probe 060.
        Burst060 = "burst_060",
        /// Short burst probe 061.
        Burst061 = "burst_061",
        /// Short burst probe 062.
        Burst062 = "burst_062",
        /// Short burst probe 063.
        Burst063 = "burst_063",
        /// Short burst probe 064.
        Burst064 = "burst_064",
        /// Short burst probe 065.
        Burst065 = "burst_065",
        /// Short burst probe 066.
        Burst066 = "burst_066",
        /// Short burst probe 067.
        Burst067 = "burst_067",
        /// Short burst probe 068.
        Burst068 = "burst_068",
        /// Short burst probe 069.
        Burst069 = "burst_069",
        /// Short burst probe 070.
        Burst070 = "burst_070",
        /// Short burst probe 071.
        Burst071 = "burst_071",
        /// Short burst probe 072.
        Burst072 = "burst_072",
        /// Short burst probe 073.
        Burst073 = "burst_073",
        /// Short burst probe 074.
        Burst074 = "burst_074",
        /// Short burst probe 075.
        Burst075 = "burst_075",
        /// Short burst probe 076.
        Burst076 = "burst_076",
        /// Short burst probe 077.
        Burst077 = "burst_077",
        /// Short burst probe 078.
        Burst078 = "burst_078",
        /// Short burst probe 079.
        Burst079 = "burst_079",
        /// Short burst probe 080.
        Burst080 = "burst_080",
        /// Short burst probe 081.
        Burst081 = "burst_081",
        /// Short burst probe 082.
        Burst082 = "burst_082",
        /// Short burst probe 083.
        Burst083 = "burst_083",
        /// Short burst probe 084.
        Burst084 = "burst_084",
        /// Short burst probe 085.
        Burst085 = "burst_085",
        /// Short burst probe 086.
        Burst086 = "burst_086",
        /// Short burst probe 087.
        Burst087 = "burst_087",
        /// Short burst probe 088.
        Burst088 = "burst_088",
        /// Short burst probe 089.
        Burst089 = "burst_089",
        /// Short burst probe 090.
        Burst090 = "burst_090",
        /// Short burst probe 091.
        Burst091 = "burst_091",
        /// Short burst probe 092.
        Burst092 = "burst_092",
        /// Short burst probe 093.
        Burst093 = "burst_093",
        /// Short burst probe 094.
        Burst094 = "burst_094",
        /// Short burst probe 095.
        Burst095 = "burst_095",
        /// Short burst probe 096.
        Burst096 = "burst_096",
        /// Short burst probe 097.
        Burst097 = "burst_097",
        /// Short burst probe 098.
        Burst098 = "burst_098",
        /// Short burst probe 099.
        Burst099 = "burst_099",
        /// Short burst probe 100.
        Burst100 = "burst_100",
        /// Short burst probe 101.
        Burst101 = "burst_101",
        /// Short burst probe 102.
        Burst102 = "burst_102",
        /// Short burst probe 103.
        Burst103 = "burst_103",
        /// Short burst probe 104.
        Burst104 = "burst_104",
        /// Short burst probe 105.
        Burst105 = "burst_105",
        /// Short burst probe 106.
        Burst106 = "burst_106",
        /// Short burst probe 107.
        Burst107 = "burst_107",
        /// Short burst probe 108.
        Burst108 = "burst_108",
        /// Short burst probe 109.
        Burst109 = "burst_109",
        /// Short burst probe 110.
        Burst110 = "burst_110",
        /// Short burst probe 111.
        Burst111 = "burst_111",
        /// Short burst probe 112.
        Burst112 = "burst_112",
        /// Short burst probe 113.
        Burst113 = "burst_113",
        /// Short burst probe 114.
        Burst114 = "burst_114",
        /// Short burst probe 115.
        Burst115 = "burst_115",
        /// Short burst probe 116.
        Burst116 = "burst_116",
        /// Short burst probe 117.
        Burst117 = "burst_117",
        /// Short burst probe 118.
        Burst118 = "burst_118",
        /// Short burst probe 119.
        Burst119 = "burst_119",
        /// Short burst probe 120.
        Burst120 = "burst_120",
        /// Short burst probe 121.
        Burst121 = "burst_121",
        /// Short burst probe 122.
        Burst122 = "burst_122",
        /// Short burst probe 123.
        Burst123 = "burst_123",
        /// Short burst probe 124.
        Burst124 = "burst_124",
        /// Short burst probe 125.
        Burst125 = "burst_125",
        /// Short burst probe 126.
        Burst126 = "burst_126",
        /// Short burst probe 127.
        Burst127 = "burst_127",
        /// Short burst probe 128.
        Burst128 = "burst_128",
        /// Short burst probe 129.
        Burst129 = "burst_129",
        /// Short burst probe 130.
        Burst130 = "burst_130",
        /// Short burst probe 131.
        Burst131 = "burst_131",
        /// Short burst probe 132.
        Burst132 = "burst_132",
        /// Short burst probe 133.
        Burst133 = "burst_133",
        /// Short burst probe 134.
        Burst134 = "burst_134",
        /// Short burst probe 135.
        Burst135 = "burst_135",
        /// Short burst probe 136.
        Burst136 = "burst_136",
        /// Short burst probe 137.
        Burst137 = "burst_137",
        /// Short burst probe 138.
        Burst138 = "burst_138",
        /// Short burst probe 139.
        Burst139 = "burst_139",
        /// Short burst probe 140.
        Burst140 = "burst_140",
        /// Short burst probe 141.
        Burst141 = "burst_141",
        /// Short burst probe 142.
        Burst142 = "burst_142",
        /// Short burst probe 143.
        Burst143 = "burst_143",
        /// Short burst probe 144.
        Burst144 = "burst_144",
        /// Short burst probe 145.
        Burst145 = "burst_145",
        /// Short burst probe 146.
        Burst146 = "burst_146",
        /// Short burst probe 147.
        Burst147 = "burst_147",
        /// Short burst probe 148.
        Burst148 = "burst_148",
        /// Short burst probe 149.
        Burst149 = "burst_149",
        /// Short burst probe 150.
        Burst150 = "burst_150",
        /// Short burst probe 151.
        Burst151 = "burst_151",
        /// Short burst probe 152.
        Burst152 = "burst_152",
        /// Short burst probe 153.
        Burst153 = "burst_153",
        /// Short burst probe 154.
        Burst154 = "burst_154",
        /// Short burst probe 155.
        Burst155 = "burst_155",
        /// Short burst probe 156.
        Burst156 = "burst_156",
        /// Short burst probe 157.
        Burst157 = "burst_157",
        /// Short burst probe 158.
        Burst158 = "burst_158",
        /// Short burst probe 159.
        Burst159 = "burst_159",
        /// Held work probe 0.
        WorkHeld0 = "work_held_0",
        /// Held work probe 1.
        WorkHeld1 = "work_held_1",
        /// Held work probe 2.
        WorkHeld2 = "work_held_2",
        /// Short work probe for training and queueing.
        WorkPlain = "work_plain",
        /// Reports to the journal, waits for the go flag, then allocates far beyond its hard
        /// memory ceiling.
        HeldHog = "held_hog",
    }
}
impl Probe {
    /// The Bazel flags building the target needs beyond the suite's own.
    #[must_use]
    pub const fn build_flags(self) -> &'static [&'static str] {
        match self {
            // Only under this layout does Bazel give the action a working directory below its
            // input root (the execroot's name), with external repositories beside it.
            Self::InputRoot => &["--experimental_sibling_repository_layout"],
            _ => &[],
        }
    }

    /// The Bazel label of the target.
    #[must_use]
    pub fn label(self) -> String {
        format!("//:{}", self.as_str())
    }

    /// The path of the target's report under the workspace, through the `bazel-bin` symlink.
    #[must_use]
    pub fn report_path(self) -> String {
        format!("bazel-bin/{}.report", self.as_str())
    }
}

/// A running Gang deployment under one [`Variant`], and the Bazel workspace that uses it.
///
/// Methods take `&self` and the trait is `Sync`, so a scenario can run a build on one thread
/// while it kills and restarts the server on another.
pub trait System: Sync {
    /// Starts `gang server` and returns once its client listener accepts connections.
    ///
    /// # Errors
    ///
    /// When the server cannot be started or does not start listening.
    fn start_server(&self) -> Result<()>;

    /// Kills `gang server` with `SIGKILL`; workers it started keep running, as instances do.
    ///
    /// # Errors
    ///
    /// When the server cannot be killed.
    fn kill_server(&self) -> Result<()>;

    /// The process id of the running server, or `None` when it is not running.
    fn server_pid(&self) -> Option<u32>;

    /// The number of `gang worker` processes of this deployment.
    ///
    /// # Errors
    ///
    /// When the process table cannot be read.
    fn worker_count(&self) -> Result<usize>;

    /// Every scaling event the server recorded, oldest first.
    ///
    /// # Errors
    ///
    /// When the server's scaling history cannot be read.
    fn scaling_events(&self) -> Result<Vec<Event>>;

    /// The HTTP status an anonymous request to the events list answers, over plain HTTP. The
    /// plaintext deployment binds `none:anonymous` to the administrator role, so the route must
    /// admit the caller with `200`.
    ///
    /// # Errors
    ///
    /// When the system has no plaintext API or the request cannot be made.
    fn anonymous_admin_status(&self) -> Result<u16>;

    /// The live state, as `GET /v1/state` answers it to the deployment's own administrator: an
    /// anonymous caller of a plaintext deployment, the administrator's bearer token otherwise.
    ///
    /// # Errors
    ///
    /// When the state cannot be requested or the answer is not a state.
    fn live_state(&self) -> Result<State>;

    /// Opens `GET /v1/state:watch` as [`System::live_state`] reads the state and returns every
    /// frame it carried, oldest first, once `done` holds for them.
    ///
    /// `done` is evaluated on the frames read so far after each new frame, starting with the
    /// snapshot. The wait is bounded by [`Settings::history_timeout`](crate::settings::Settings),
    /// one deadline for the whole wait; ticks do not extend it. A stream that ends before `done`
    /// holds is an error.
    ///
    /// # Errors
    ///
    /// When the stream cannot be opened or does not answer with `200`, cannot be decoded, ends
    /// before `done` holds, or `done` does not hold by the deadline ([`E2eError::Timeout`]).
    fn await_stream(&self, done: &mut dyn FnMut(&[Frame]) -> bool) -> Result<Vec<Frame>>;

    /// Builds `probes` in the sample workspace against the remote executor and returns how it
    /// ended. A build that fails is `Ok` with a failed [`BuildOutcome`].
    ///
    /// # Errors
    ///
    /// When Bazel cannot be run.
    fn build(&self, probes: &[Probe]) -> Result<BuildOutcome>;

    /// Builds `probes` as `run`: their actions are distinct from those of every other build, so
    /// no earlier result or attempt stands in for them, and the journal lines their probes write
    /// start with `run` and a space. A build that fails is `Ok` with a failed [`BuildOutcome`].
    ///
    /// # Errors
    ///
    /// When Bazel cannot be run.
    fn build_run(&self, probes: &[Probe], run: &Run) -> Result<BuildOutcome>;

    /// Removes the workspace's build outputs (`bazel clean`), keeping the remote cache.
    ///
    /// # Errors
    ///
    /// When Bazel cannot be run.
    fn clean(&self) -> Result<()>;

    /// The report a built probe wrote.
    ///
    /// # Errors
    ///
    /// When the probe has not been built or its report cannot be parsed.
    fn report(&self, probe: Probe) -> Result<ProbeReport>;

    /// The namespaces and paths of the machine the harness runs on.
    fn host(&self) -> &Host;

    /// Returns the whole recorded scaling history, oldest first, once `done` holds for it.
    ///
    /// `done` is evaluated on the history read so far first, then after each frame of the state
    /// stream that follows it. The wait is on the stream, bounded by `Settings::history_timeout`.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`](crate::E2eError::Timeout) when `done` still rejects the history once
    /// the timeout has passed; otherwise when the system cannot be observed.
    fn await_history(&self, done: &mut dyn FnMut(&[Event]) -> bool) -> Result<Vec<Event>>;

    /// The directory the bwrap sandbox mounts as an empty tmpfs.
    fn tmp_path(&self) -> &Path;

    /// How many trailing lines of a process's stderr a failed check quotes.
    fn stderr_tail_lines(&self) -> usize;
}

/// The cgroup scope of a running action and the limits the kernel applied to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeFacts {
    /// The cgroup directory of the running action.
    pub directory: PathBuf,
    /// The working directory of the running action.
    pub working_directory: PathBuf,
    /// The trimmed contents of `memory.max`.
    pub memory_max: String,
    /// The trimmed contents of `memory.oom.group`.
    pub oom_group: String,
    /// The trimmed contents of `cpu.max`.
    pub cpu_max: String,
    /// The trimmed contents of `pids.max`.
    pub pids_max: String,
}

/// One action start recorded by a memory probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    /// Stable probe action identifier.
    pub action: String,
    /// Scheduler-provided memory reservation, in KiB.
    pub memory_kb: u64,
}

/// Evidence gathered while the worker action pool is under memory pressure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PressureObservation {
    /// Exact victim cgroup peak, rounded up to KiB before the kill.
    pub victim_peak_kb: u64,
    /// Whether the victim's scope was killed.
    pub victim_killed: bool,
    /// Whether the neighbour survived at least three pressure-kill windows after restoration.
    pub neighbour_alive: bool,
}

/// Observes and releases the per-action limits of a [`System`] whose workers enforce them.
pub trait Limits: System {
    /// The scope of the hold probe's orphan process, or `None` until it runs in an action scope.
    ///
    /// # Errors
    ///
    /// When the process table or the cgroup files cannot be read.
    fn running_scope(&self) -> Result<Option<ScopeFacts>>;

    /// Lets the hold probe finish.
    ///
    /// # Errors
    ///
    /// When the release marker cannot be written.
    fn release(&self, scope: &ScopeFacts) -> Result<()>;

    /// Whether the hold probe's orphan process exists.
    ///
    /// # Errors
    ///
    /// When the process table cannot be read.
    fn orphan_alive(&self) -> Result<bool>;

    /// Whether the cgroup directory of `scope` still exists.
    ///
    /// # Errors
    ///
    /// When the filesystem cannot be queried.
    fn scope_exists(&self, scope: &ScopeFacts) -> Result<bool>;

    /// Blocks until the hold probe's orphan starts or the hold build is abandoned, and reports
    /// which happened.
    ///
    /// # Errors
    ///
    /// [`E2eError::Timeout`] when the hold action does not signal within the settings' hold
    /// timeout.
    ///
    /// When the signal pipe cannot be read or contains an unexpected byte.
    fn await_hold(&self) -> Result<crate::Signalled>;

    /// Wakes a pending or future `await_hold` with abandonment; does not end the hold build.
    ///
    /// # Errors
    ///
    /// When the signal pipe cannot be written.
    fn abandon_hold(&self) -> Result<()>;

    /// Ends the running hold build without releasing its action, whether or not the action runs
    /// in a scope; a hold build that already ended is not an error. Unlike
    /// [`Limits::abandon_hold`], this ends the build itself.
    ///
    /// # Errors
    ///
    /// When the build cannot be stopped.
    fn cancel_hold(&self) -> Result<()>;

    /// Reads the action identifier and reservation recorded for each attempt.
    ///
    /// # Errors
    ///
    /// When the records cannot be read or contain an invalid reservation.
    fn attempt_records(&self) -> Result<Vec<AttemptRecord>>;

    /// Induces genuine pool PSI and observes the victim and its neighbour.
    ///
    /// # Errors
    ///
    /// When the scopes cannot be observed or their pool cannot be controlled.
    fn induce_pressure(&self, victim_pid: u32, neighbour_pid: u32) -> Result<PressureObservation>;

    /// Waits for a kernel OOM kill in the pool shared by the two probe processes.
    ///
    /// # Errors
    ///
    /// When the pool or its memory events cannot be observed.
    fn await_pool_oom(&self, victim_pid: u32, neighbour_pid: u32) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::Probe;

    #[test]
    fn probe_names_are_unique_and_parse_back() {
        for probe in Probe::ALL {
            assert_eq!(probe.as_str().parse::<Probe>(), Ok(probe));
        }

        let mut names = Probe::ALL.map(Probe::as_str);
        names.sort_unstable();
        assert!(names.windows(2).all(|pair| pair[0] != pair[1]));
    }
}
