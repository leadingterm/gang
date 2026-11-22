### Fixed

- Account for each usable physical worker's advertised CPU, memory and disk capacity when scaling, while retaining conservative estimates for pending capacity and counting overlapping connections once.
- Protect each action with its own measured memory peak, while retaining target, command and mnemonic history as predictive fallbacks rather than hard floors.
- Preserve the v0.3.8 resource-history baseline for rollback and exchange observed capacity safely across mixed-version controllers.
