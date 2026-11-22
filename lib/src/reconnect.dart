import 'dart:math';

/// What the client does after the server closed the connection with a code.
enum PusherCloseAction {
  /// Reconnect after a short jittered delay, then back off.
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
///   4201 = pong not received); the client still waits a short random delay
///   so that all clients don't come back at once after a deploy.
///
/// Any other code (none, 1000, 1001, 1006, ...) is a lost connection, which the
/// client always re-establishes.
PusherCloseAction closeActionFor(int? code) {
  if (code == null) return PusherCloseAction.reconnect;
  if (code >= 4000 && code <= 4099) return PusherCloseAction.doNotReconnect;
  if (code >= 4100 && code <= 4199) return PusherCloseAction.reconnectWithBackoff;
  return PusherCloseAction.reconnect;
}

/// Jittered exponential backoff between reconnection attempts.
///
/// The first attempt after a healthy connection waits a random delay between
/// zero and twice [minDelay] ("full jitter"), so clients that lost their
/// connection at the same moment (a server deploy) don't all come back at the
/// same moment. Attempt `n` after that waits a random delay between half and
/// all of `min(maxDelay, minDelay * 2^n)` ("equal jitter"), so it never exceeds
/// [maxDelay] and never gives up.
class ReconnectBackoff {
  ReconnectBackoff({required this.minDelay, required this.maxDelay, Random? random}) : _random = random ?? Random();

  /// The base delay of the first attempts.
  final Duration minDelay;

  /// The longest delay between two attempts.
  final Duration maxDelay;

  final Random _random;

  int _attempt = 0;

  /// The number of delays handed out since the last [reset].
  int get attempt => _attempt;

  /// The delay before the next attempt. [minAttempt] skips the first steps,
  /// e.g. 1 to never retry with less than [minDelay].
  Duration next({int minAttempt = 0}) {
    if (_attempt < minAttempt) _attempt = minAttempt;
    final maxMs = maxDelay.inMilliseconds;
    final Duration delay;
    if (_attempt == 0) {
      delay = Duration(milliseconds: _random.nextInt(min(minDelay.inMilliseconds * 2, maxMs) + 1));
    } else {
      final baseMs = min(minDelay.inMilliseconds * pow(2, min(_attempt, 30)), maxMs).toInt();
      final halfMs = baseMs ~/ 2;
      delay = Duration(milliseconds: halfMs + _random.nextInt(baseMs - halfMs + 1));
    }
    _attempt++;
    return delay;
  }

  /// Starts over: the next delay is a first attempt's.
  void reset() => _attempt = 0;
}
