import 'package:web_socket_channel/web_socket_channel.dart';

/// A WebSocket connection attempt.
class Dial {
  Dial(this.channel, this._abort);

  /// The connection; [WebSocketChannel.ready] completes once it is open.
  final WebSocketChannel channel;

  final void Function() _abort;

  /// Gives up on the attempt: closes the connection, also when it is still
  /// being established (its TCP connection or upgrade request included).
  void abort() => _abort();
}
