import 'dart:async';
import 'package:resilient_offline_queue/resilient_offline_queue.dart';

void main() async {
  // 1. Initialize Mock Adapters
  final storage = MemoryQueueStorage();
  final networkMonitor = SimpleNetworkMonitor();

  // 2. Initialize the OfflineQueue Engine
  final offlineQueue = OfflineQueue(
    storage: storage,
    networkMonitor: networkMonitor,
    executor: (item) async {
      print('Executing item [${item.id}]: ${item.method} to ${item.url}');
      // Here, you would call your HTTP client (e.g. dio.post, http.put)
      // return true for success, false to trigger exponential retry backoff
      return true; 
    },
  );

  // Initialize background listeners
  offlineQueue.initialize();

  print('Queue active. Current pending count: ${await storage.count()}');

  // 3. Enqueue requests
  print('Device goes offline...');
  networkMonitor.setConnected(false);

  print('Enqueueing product update request...');
  await offlineQueue.enqueue(
    id: 'req-001',
    url: 'https://api.example.com/v1/materials/cimento',
    method: 'PATCH',
    body: '{"stock": 150}',
  );

  print('Enqueueing notification logs...');
  await offlineQueue.enqueue(
    id: 'req-002',
    url: 'https://api.example.com/v1/logs',
    method: 'POST',
    body: '{"message": "Stock check performed"}',
  );

  print('Current pending count: ${await storage.count()}'); // Outputs: 2

  // 4. Trigger internet connection back online
  print('Internet restored. Syncing...');
  networkMonitor.setConnected(true);

  // Let the asynchronous microtask loops complete
  await Future.delayed(const Duration(milliseconds: 100));

  print('Post-sync pending count: ${await storage.count()}'); // Outputs: 0

  // Dispose queue listeners
  offlineQueue.dispose();
}

// ── Concrete implementations for illustration ───────────────────

class SimpleNetworkMonitor implements NetworkMonitor {
  bool _connected = true;
  final _controller = StreamController<bool>.broadcast();

  void setConnected(bool value) {
    _connected = value;
    _controller.add(value);
  }

  @override
  Future<bool> get isConnected async => _connected;

  @override
  Stream<bool> get onConnectivityChanged => _controller.stream;
}
