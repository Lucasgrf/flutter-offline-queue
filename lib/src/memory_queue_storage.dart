import 'queue_item.dart';
import 'queue_storage.dart';

/// An in-memory implementation of [QueueStorage].
///
/// Useful for testing, local prototyping, or development environments 
/// where persistent disk storage is not immediately required.
class MemoryQueueStorage implements QueueStorage {
  final Map<String, QueueItem> _db = {};

  @override
  Future<void> save(QueueItem item) async {
    _db[item.id] = item;
  }

  @override
  Future<void> delete(String id) async {
    _db.remove(id);
  }

  @override
  Future<void> update(QueueItem item) async {
    _db[item.id] = item;
  }

  @override
  Future<List<QueueItem>> getAllPending() async {
    final list = _db.values.toList();
    list.sort((a, b) => a.timestamp.compareTo(b.timestamp));
    return list;
  }

  @override
  Future<int> count() async => _db.length;

  @override
  Future<void> clear() async => _db.clear();
}
