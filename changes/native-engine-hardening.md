### Fixed

- Derive scheduler and scaling CPU, memory, and disk capacity from registered workers, preserving generation ordering and independent action ceilings.
- Cancel cache fetches and tree builds before draining worker caches, and join outstanding filesystem work before retiring their directories.
- Mount only empty, descriptor-verified action scratch directories as ephemeral Bubblewrap tmpfs, preserving staged inputs and declared outputs.
- Account for sandbox processes across new sessions using the trusted accounting PID and proportional memory, without an RSS fallback.
- Drain surviving action descendants under Linux subreaper ownership. On macOS, cleanup covers the original process group; descendants that detach into another group or session remain outside that guarantee.
