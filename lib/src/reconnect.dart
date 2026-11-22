import 'dart:math';

import 'package:web_socket_client/web_socket_client.dart' show Backoff;

/// What the client does after the server closed the connection with a code.
enum PusherCloseAction {
  /// Reconnect right away (web_socket_client re-dials immediately, then backs off).
  reconnect,

  /// Reconnect after backing off.
  reconnectWithBackoff,

  /// Do not reconnect: the server refused the connection for good.
  doNotReconnect,
}

/// Maps a WebSocket close code to what the client does next, following the
/// Pusher protocol's close code ranges:
///
/// * 4000-4099: the connection must not be re-established (e.g. app key
///   wrong, over quota).
/// * 4100-4199: reconnect after backing off (e.g. over capacity).
/// * 4200-4299: reconnect immediately (4200 = server shutting down,
///   4201 = pong not received).
///
/// Any other code (none, 1000, 1001, 1006, ...) is a lost connection, which the
/// client always re-establishes.
PusherCloseAction closeActionFor(int? code) {
  if (code == null) return PusherCloseAction.reconnect;
  if (code >= 4000 && code <= 4099) return PusherCloseAction.doNotReconnect;
  if (code >= 4100 && code <= 4199) return PusherCloseAction.reconnectWithBackoff;
  return PusherCloseAction.reconnect;
}

/// Exponential backoff with "equal jitter": attempt `n` waits a random delay
/// between half and all of `min(maxDelay, minDelay * 2^n)`, so it never
/// exceeds [maxDelay] and clients that lost their connection at the same time
/// (a server deploy) don't all come back at the same moment.
///
/// It is shared by every socket of a client: web_socket_client uses it between
/// its own reconnection attempts, and the client between re-created sockets.
/// web_socket_client calls [reset] as soon as a WebSocket handshake succeeds,
/// which is ignored: only a Pusher handshake (`pusher:connection_established`)
/// counts as a successful connection, and the client then calls [restart].
class ReconnectBackoff implements Backoff {
  ReconnectBackoff({required this.minDelay, required this.maxDelay, Random? random}) : _random = random ?? Random();

  /// The base delay of the first attempt.
  final Duration minDelay;

  /// The longest delay between two attempts.
  final Duration maxDelay;

  final Random _random;

  int _attempt = 0;

  /// The number of delays handed out since the last [restart].
  int get attempt => _attempt;

  @override
  Duration next() {
    final maxMs = maxDelay.inMilliseconds;
    final baseMs = min(minDelay.inMilliseconds * pow(2, min(_attempt, 30)), maxMs).toInt();
    _attempt++;
    final halfMs = baseMs ~/ 2;
    return Duration(milliseconds: halfMs + _random.nextInt(baseMs - halfMs + 1));
  }

  /// Ignored, see the class documentation.
  @override
  void reset() {}

  /// Starts over from [minDelay].
  void restart() => _attempt = 0;
}
