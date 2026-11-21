/// A clean, modular, and robust offline synchronization queue library.
///
/// Abstrai a fila local de requisições, permitindo sincronização offline-first
/// automática por monitoramento de rede e retentativas com backoff exponencial.
library resilient_offline_queue;

export 'src/queue_item.dart';
export 'src/queue_storage.dart';
export 'src/network_monitor.dart';
export 'src/offline_queue.dart';
export 'src/memory_queue_storage.dart';
