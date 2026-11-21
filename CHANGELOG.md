## 1.0.0

- Initial release of `resilient_offline_queue`.
- Storage-agnostic `QueueStorage` and `MemoryQueueStorage` contracts.
- Connectivity-aware `NetworkMonitor` integration.
- Strict FIFO `OfflineQueue` execution engine with synchronization lock safety.
- Exponential backoff retry policy with randomized ±10% jitter.
- Unit test suite covering offline queuing, recovery transitions, temporary failures, unrecoverable errors, and queue clearing.
