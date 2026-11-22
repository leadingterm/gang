### Fixed

- Keep controller takeover preparation outside lease deadlines and retry lifecycle notifications without interrupting serving traffic.
- Cancel a deployment refresh when its incoming controller cannot serve within ten minutes, without retrying the failed refresh.
