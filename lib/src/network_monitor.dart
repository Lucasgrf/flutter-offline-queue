/// Contract to monitor internet connectivity changes on the client device.
abstract class NetworkMonitor {
  /// Stream that emits `true` when device goes online, and `false` when offline.
  Stream<bool> get onConnectivityChanged;

  /// Checks the current immediate connectivity status of the device.
  Future<bool> get isConnected;
}
