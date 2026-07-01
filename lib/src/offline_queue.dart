import 'dart:async';
import 'dart:math';
import 'queue_item.dart';
import 'queue_storage.dart';
import 'network_monitor.dart';

/// Callback type signature to execute pending queue items.
///
/// Returns `true` if the item executed successfully (and should be deleted),
/// or `false` if it encountered a temporary network/server issue and needs a retry.
///
/// Unrecoverable client errors (e.g., 400 Bad Request, 403 Forbidden) should throw
/// an exception or return `true` to discard the malformed payload from the queue.
typedef QueueExecutor = Future<bool> Function(QueueItem item);

/// The central engine coordinating offline queue persistence and synchronization.
class OfflineQueue {
  final QueueStorage _storage;
  final NetworkMonitor _networkMonitor;
  final QueueExecutor _executor;
  
  final Duration _baseBackoff;
  final int _maxAttempts;

  bool _isProcessing = false;
  StreamSubscription<bool>? _connectivitySub;

  OfflineQueue({
    required QueueStorage storage,
    required NetworkMonitor networkMonitor,
    required QueueExecutor executor,
    Duration baseBackoff = const Duration(seconds: 2),
    int maxAttempts = 5,
  })  : _storage = storage,
        _networkMonitor = networkMonitor,
        _executor = executor,
        _baseBackoff = baseBackoff,
        _maxAttempts = maxAttempts;

  /// Starts listening to network change events to trigger automatic sync loops.
  void initialize() {
    _connectivitySub = _networkMonitor.onConnectivityChanged.listen((isOnline) {
      if (isOnline) {
        triggerSync();
      }
    });
  }

  /// Cancels network listeners. Call this when disposing controllers/blocs.
  void dispose() {
    _connectivitySub?.cancel();
  }

  /// Enqueues a new mutation to local storage.
  ///
  /// If immediate connectivity is available, it executes immediately.
  /// Otherwise, it saves locally to be processed when network returns.
  Future<void> enqueue({
    required String id,
    required String url,
    required String method,
    Map<String, String>? headers,
    String? body,
  }) async {
    final item = QueueItem(
      id: id,
      url: url,
      method: method,
      headers: headers ?? const {},
      body: body,
      timestamp: DateTime.now(),
    );

    await _storage.save(item);

    final online = await _networkMonitor.isConnected;
    if (online) {
      triggerSync();
    }
  }

  /// Manually triggers processing of enqueued offline mutations.
  ///
  /// Prevents concurrent processing loops using a state lock.
  Future<void> triggerSync() async {
    if (_isProcessing) return;
    _isProcessing = true;

    try {
      await _processQueue();
    } finally {
      _isProcessing = false;
    }
  }

  /// Sequentially executes pending queue items in FIFO order.
  Future<void> _processQueue() async {
    while (true) {
      // Fetch latest pending elements
      final pending = await _storage.getAllPending();
      if (pending.isEmpty) break;

      // Ensure we are still online
      final online = await _networkMonitor.isConnected;
      if (!online) break;

      final item = pending.first;

      try {
        final success = await _executor(item);

        if (success) {
          // Success: delete from storage and continue loop
          await _storage.delete(item.id);
        } else {
          // Temporary failure: reschedule with backoff and stop queue processing
          await _handleTemporaryFailure(item);
          break;
        }
      } catch (e) {
        // Critical exception: discard malformed item or log to prevent blockages
        await _storage.delete(item.id);
        // Resumes queue processing for remaining elements
      }
    }
  }

  /// Updates attempts and schedules a delay before processing restarts.
  Future<void> _handleTemporaryFailure(QueueItem item) async {
    if (item.attempts >= _maxAttempts) {
      // Discard item if retry limit is exceeded (prevents infinite lockups)
      await _storage.delete(item.id);
      return;
    }

    final updated = item.copyWithAttempt();
    await _storage.update(updated);

    // Calculate exponential backoff time: base * 2^attempts + jitter
    final exponent = min(updated.attempts, 6); // Cap exponent to avoid overflow
    final backoffMillis = _baseBackoff.inMilliseconds * pow(2, exponent);
    
    // Add minor random jitter (±10%) to prevent synchronized thundering herd problems
    final jitter = (Random().nextDouble() * 0.2 - 0.1) * backoffMillis;
    final finalDelay = Duration(milliseconds: (backoffMillis + jitter).round());

    // Delay future triggers
    Future.delayed(finalDelay, () {
      triggerSync();
    });
  }
}
