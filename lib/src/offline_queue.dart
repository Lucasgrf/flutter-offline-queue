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
  final void Function(String message)? _logger;

  bool _isProcessing = false;
  bool _disposed = false;
  StreamSubscription<bool>? _connectivitySub;

  OfflineQueue({
    required QueueStorage storage,
    required NetworkMonitor networkMonitor,
    required QueueExecutor executor,
    Duration baseBackoff = const Duration(seconds: 2),
    int maxAttempts = 5,
    void Function(String message)? logger,
  })  : _storage = storage,
        _networkMonitor = networkMonitor,
        _executor = executor,
        _baseBackoff = baseBackoff,
        _maxAttempts = maxAttempts,
        _logger = logger;

  /// Starts listening to network change events to trigger automatic sync loops.
  void initialize() {
    _logger?.call('Initializing OfflineQueue: listening to connectivity changes.');
    _connectivitySub = _networkMonitor.onConnectivityChanged.listen((isOnline) {
      _logger?.call('Connectivity changed: isOnline = $isOnline');
      if (isOnline) {
        triggerSync();
      }
    });
  }

  /// Cancels network listeners. Call this when disposing controllers/blocs.
  void dispose() {
    _logger?.call('Disposing OfflineQueue background listeners.');
    _disposed = true;
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

    _logger?.call('Enqueuing mutation item [${item.id}]: ${item.method} to ${item.url}');
    await _storage.save(item);

    final online = await _networkMonitor.isConnected;
    if (online) {
      _logger?.call('Device is online, triggering sync immediately.');
      triggerSync();
    } else {
      _logger?.call('Device is offline, mutation item cached locally.');
    }
  }

  /// Manually triggers processing of enqueued offline mutations.
  ///
  /// Prevents concurrent processing loops using a state lock.
  Future<void> triggerSync() async {
    if (_isProcessing) {
      _logger?.call('Sync already in progress, skipping trigger.');
      return;
    }
    _isProcessing = true;

    _logger?.call('Starting offline queue synchronization routine.');
    try {
      await _processQueue();
    } finally {
      _isProcessing = false;
      _logger?.call('Offline queue synchronization routine finished.');
    }
  }

  /// Sequentially executes pending queue items in FIFO order.
  Future<void> _processQueue() async {
    while (true) {
      // Fetch latest pending elements
      final pending = await _storage.getAllPending();
      if (pending.isEmpty) {
        _logger?.call('No pending items in queue.');
        break;
      }

      // Ensure we are still online
      final online = await _networkMonitor.isConnected;
      if (!online) {
        _logger?.call('Device lost connectivity during sync, aborting.');
        break;
      }

      final item = pending.first;
      _logger?.call('Processing item [${item.id}]: executing network call...');

      try {
        final success = await _executor(item);

        if (success) {
          _logger?.call('Item [${item.id}] executed successfully. Removing from cache.');
          await _storage.delete(item.id);
        } else {
          _logger?.call('Item [${item.id}] failed due to temporary issues.');
          await _handleTemporaryFailure(item);
          break;
        }
      } catch (e) {
        _logger?.call('Item [${item.id}] threw unrecoverable exception: $e. Discarding to avoid queue lock.');
        await _storage.delete(item.id);
      }
    }
  }

  /// Updates attempts and schedules a delay before processing restarts.
  Future<void> _handleTemporaryFailure(QueueItem item) async {
    if (item.attempts >= _maxAttempts) {
      _logger?.call('Item [${item.id}] exceeded maximum retry attempts ($_maxAttempts). Discarding.');
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

    _logger?.call('Rescheduling sync trigger for item [${item.id}] in ${finalDelay.inMilliseconds}ms (backoff: ${updated.attempts} attempt).');

    // Delay future triggers
    Future.delayed(finalDelay, () {
      if (!_disposed) {
        triggerSync();
      }
    });
  }
}
