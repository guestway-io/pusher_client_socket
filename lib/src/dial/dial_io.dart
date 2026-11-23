import 'dart:io';

import 'package:web_socket_channel/io.dart';

import 'dial.dart';

/// Dials [uri] with an `HttpClient` of its own, so that giving up on the
/// attempt also closes a TCP connection or upgrade request that is still
/// pending (a server that accepts the connection but never answers).
Dial dial(Uri uri, Duration connectTimeout) {
  final client = HttpClient()..connectionTimeout = connectTimeout;
  final webSocket = WebSocket.connect(uri.toString(), customClient: client);
  // Once upgraded, the WebSocket no longer depends on the client.
  webSocket.then((_) => client.close(), onError: (_) => client.close(force: true));
  final channel = IOWebSocketChannel(webSocket);
  return Dial(channel, () {
    client.close(force: true);
    channel.sink.close().catchError((_) {});
  });
}
