### Changed

- Move worker action-directory cleanup off the execution critical path after `cache_action_result` and `execution_response`, while keeping operation-id reuse, single-use worker exit, and graceful shutdown drain synchronized on cleanup completion.
