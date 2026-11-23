import 'package:web_socket_channel/web_socket_channel.dart';

import 'dial.dart';

/// Dials [uri] (browsers: the WebSocket closes once its sink is closed, even
/// while it is still connecting).
Dial dial(Uri uri, Duration connectTimeout) {
  final channel = WebSocketChannel.connect(uri);
  return Dial(channel, () => channel.sink.close().catchError((_) {}));
}
