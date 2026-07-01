import 'dart:convert';

/// Represents a pending network mutation enqueued while the device was offline.
class QueueItem {
  final String id;
  final String url;
  final String method;
  final Map<String, String> headers;
  final String? body;
  final DateTime timestamp;
  final int attempts;

  QueueItem({
    required this.id,
    required this.url,
    required this.method,
    required this.headers,
    this.body,
    required this.timestamp,
    this.attempts = 0,
  });

  /// Increments attempts count for backoff calculations.
  QueueItem copyWithAttempt() {
    return QueueItem(
      id: id,
      url: url,
      method: method,
      headers: headers,
      body: body,
      timestamp: timestamp,
      attempts: attempts + 1,
    );
  }

  Map<String, dynamic> toMap() {
    return {
      'id': id,
      'url': url,
      'method': method,
      'headers': headers,
      'body': body,
      'timestamp': timestamp.toIso8601String(),
      'attempts': attempts,
    };
  }

  factory QueueItem.fromMap(Map<String, dynamic> map) {
    return QueueItem(
      id: map['id'] as String,
      url: map['url'] as String,
      method: map['method'] as String,
      headers: Map<String, String>.from(map['headers'] as Map),
      body: map['body'] as String?,
      timestamp: DateTime.parse(map['timestamp'] as String),
      attempts: map['attempts'] as int,
    );
  }

  String toJson() => json.encode(toMap());

  factory QueueItem.fromJson(String source) =>
      QueueItem.fromMap(json.decode(source) as Map<String, dynamic>);

  @override
  String toString() {
    return 'QueueItem(id: $id, method: $method, url: $url, attempts: $attempts)';
  }
}
