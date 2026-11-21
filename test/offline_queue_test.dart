import 'dart:async';
import 'package:test/test.dart';
import 'package:resilient_offline_queue/resilient_offline_queue.dart';

void main() {
  late MockStorage storage;
  late MockNetworkMonitor networkMonitor;
  late List<QueueItem> executedItems;
  late bool Function(QueueItem) executionBehavior;

  late OfflineQueue queue;

  setUp(() {
    storage = MockStorage();
    networkMonitor = MockNetworkMonitor();
    executedItems = [];
    executionBehavior = (_) => true; // Default behavior is success

    queue = OfflineQueue(
      storage: storage,
      networkMonitor: networkMonitor,
      executor: (item) async {
        executedItems.add(item);
        return executionBehavior(item);
      },
      baseBackoff: const Duration(milliseconds: 10), // Small backoff for testing
      maxAttempts: 3,
    );
    queue.initialize();
  });

  tearDown(() {
    queue.dispose();
  });

  test('Enqueuing when offline should store the item and not execute it', () async {
    networkMonitor.isOnlineValue = false;

    await queue.enqueue(
      id: 'item-1',
      url: 'https://api.example.com/checkout',
      method: 'POST',
      body: '{"itemId": 42}',
    );

    expect(storage.items.length, 1);
    expect(storage.items.first.id, 'item-1');
    expect(executedItems.isEmpty, true);
  });

  test('Enqueuing when online should execute and delete the item immediately', () async {
    networkMonitor.isOnlineValue = true;

    await queue.enqueue(
      id: 'item-2',
      url: 'https://api.example.com/checkout',
      method: 'POST',
    );

    // Yield to the event loop
    await Future.delayed(const Duration(milliseconds: 10));

    // Should execute and then delete from storage
    expect(executedItems.length, 1);
    expect(executedItems.first.id, 'item-2');
    expect(storage.items.isEmpty, true);
  });

  test('Transitioning from offline to online should trigger queue processing', () async {
    networkMonitor.isOnlineValue = false;

    // Enqueue 2 items while offline
    await queue.enqueue(id: 'item-1', url: 'api/1', method: 'POST');
    await queue.enqueue(id: 'item-2', url: 'api/2', method: 'POST');

    expect(storage.items.length, 2);
    expect(executedItems.isEmpty, true);

    // Go online
    networkMonitor.goOnline();

    // Small delay to yield to microtask queue for async stream events
    await Future.delayed(const Duration(milliseconds: 20));

    expect(executedItems.length, 2);
    expect(executedItems[0].id, 'item-1');
    expect(executedItems[1].id, 'item-2');
    expect(storage.items.isEmpty, true);
  });

  test('Encountering temporary failure should increase attempts and stop queue', () async {
    networkMonitor.isOnlineValue = true;
    executionBehavior = (_) => false; // Temporary failure

    await queue.enqueue(id: 'item-1', url: 'api/fail', method: 'PUT');

    // Yield to the event loop
    await Future.delayed(const Duration(milliseconds: 10));

    expect(executedItems.length, 1);
    expect(storage.items.length, 1);
    expect(storage.items.first.attempts, 1); // Attempt incremented
  });

  test('Encountering unrecoverable exception should delete the item to avoid locks', () async {
    networkMonitor.isOnlineValue = true;
    
    // Executor throws unrecoverable Client Exception
    queue = OfflineQueue(
      storage: storage,
      networkMonitor: networkMonitor,
      executor: (item) async {
        executedItems.add(item);
        throw Exception('400 Bad Request');
      },
    );

    await queue.enqueue(id: 'item-bad', url: 'api/bad', method: 'POST');

    // Yield to the event loop
    await Future.delayed(const Duration(milliseconds: 10));

    expect(executedItems.length, 1);
    expect(storage.items.isEmpty, true); // Bad item deleted from queue
  });

  test('Calling clear should delete all items from queue storage', () async {
    networkMonitor.isOnlineValue = false; // Keep offline so they accumulate
    await queue.enqueue(id: 'item-1', url: 'api/1', method: 'POST');
    await queue.enqueue(id: 'item-2', url: 'api/2', method: 'POST');

    expect(storage.items.length, 2);

    await queue.clear();

    expect(storage.items.isEmpty, true);
  });
}

// ── Manual Mock Classes ───────────────────────────────────────────────

class MockStorage implements QueueStorage {
  final List<QueueItem> items = [];

  @override
  Future<void> save(QueueItem item) async {
    items.add(item);
  }

  @override
  Future<void> delete(String id) async {
    items.removeWhere((element) => element.id == id);
  }

  @override
  Future<void> update(QueueItem item) async {
    final idx = items.indexWhere((element) => element.id == item.id);
    if (idx != -1) {
      items[idx] = item;
    }
  }

  @override
  Future<List<QueueItem>> getAllPending() async {
    return List.from(items);
  }

  @override
  Future<int> count() async => items.length;

  @override
  Future<void> clear() async => items.clear();
}

class MockNetworkMonitor implements NetworkMonitor {
  bool isOnlineValue = true;
  final StreamController<bool> _controller = StreamController<bool>.broadcast();

  void goOnline() {
    isOnlineValue = true;
    _controller.add(true);
  }

  void goOffline() {
    isOnlineValue = false;
    _controller.add(false);
  }

  @override
  Future<bool> get isConnected async => isOnlineValue;

  @override
  Stream<bool> get onConnectivityChanged => _controller.stream;

  void dispose() {
    _controller.close();
  }
}
