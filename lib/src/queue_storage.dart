import 'queue_item.dart';

/// Contract for persistent local storage of offline mutations.
///
/// Implementations can back this with SQLite (Drift/Floor), Hive, 
/// Shared Preferences, or plain files.
abstract class QueueStorage {
  /// Appends a new item to local storage.
  Future<void> save(QueueItem item);

  /// Deletes an item from local storage by its unique [id].
  Future<void> delete(String id);

  /// Updates an existing item (e.g. after a retry attempt increment).
  Future<void> update(QueueItem item);

  /// Retrieves all enqueued items sorted chronologically.
  Future<List<QueueItem>> getAllPending();

  /// Gets the count of currently enqueued items.
  Future<int> count();

  /// Purges all elements in storage.
  Future<void> clear();
}
