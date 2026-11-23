import 'dart:async';
import 'dart:math';

import 'package:meta/meta.dart';

import '../src/events_listeners.collection.dart';
import '../src/reconnect.dart';
import '../pusher_client_socket.dart';

export 'private_channel.dart';
export 'private_encrypted_channel.dart';
export 'presence_channel.dart';

/// Represents a channel in Pusher.
class Channel {
  /// The pusher client.
  final PusherClient client;

  /// The name of the channel.
  final String name;

  Channel({required this.client, required this.name}) {
    bind("pusher:subscription_succeeded", _onSubscriptionSucceeded);
    bind("pusher:subscription_error", _onSubscriptionError);
  }

  /// The options of the client.
  PusherOptions get options => client.options;

  /// The authentication options of the client.
  PusherAuthOptions get authOptions => options.authOptions;

  bool _subscribed = false;

  /// Whether the channel is subscribed or not.
  bool get subscribed => _subscribed;

  bool get isPrivate => name.startsWith("private-");
  bool get isPresence => name.startsWith("presence-");
  bool get isEncrypted => name.startsWith("private-encrypted-");
  bool get isPublic => !isPrivate && !isPresence && !isEncrypted;

  /// Sets the value of the subscribed property.
  @protected
  set subscribed(bool value) {
    _subscribed = value;
  }

  /// Subscribes to the channel.
  void subscribe([bool force = false]) async {
    if (!client.connected ||
        (subscribed && !force) ||
        client.socketId == null) {
      return;
    }

    cancelSubscribeRetry();
    _subscribed = false;

    options.log("SUBSCRIBE", name);

    client.sendEvent("pusher:subscribe", {"channel": name});

    if (isPublic) _subscribed = true;
  }

  final _eventsListenersCollection = EventsListenersCollection();

  /// Binds a listener to an event.
  void bind(String event, Function listener) =>
      _eventsListenersCollection.bind(event, listener);

  /// Unbinds a listener from an event.
  void unbind(String event) => _eventsListenersCollection.unbindAll(event);

  /// Handles an event.
  void handleEvent(String event, dynamic data) {
    options.log("EVENT", name, "event: $event\n  data: $data");

    _eventsListenersCollection.handleEvent(event, data);
  }

  /// Unsubscribes from the channel.
  void unsubscribe() {
    cancelSubscribeRetry();

    client.sendEvent("pusher:unsubscribe", {"channel": name});

    _subscribed = false;

    client.channelsCollection.remove(name);
  }

  /// Binding for the subscription success event.
  void onSubscriptionSuccess(Function listener) =>
      bind("pusher:subscription_succeeded", listener);

  /// Binding for the subscription error event: the auth request failed, or
  /// the server refused the subscription. The channel retries by itself while
  /// the connection lasts, and the next connection subscribes again anyway.
  void onSubscriptionError(Function listener) =>
      bind("pusher:subscription_error", listener);

  Timer? _subscribeRetryTimer;

  late final ReconnectBackoff _subscribeRetryBackoff = ReconnectBackoff(
    minDelay: options.minReconnectDelay,
    maxDelay: options.maxReconnectDelay,
  );

  /// The connection the retry backoff counts for: it starts over on a new one.
  String? _subscribeRetrySocketId;

  /// Auth requests refused in a row with 401/403.
  int _authRejections = 0;

  /// After this many 401/403 in a row, retry only every
  /// [PusherOptions.rejectedAuthRetryDelay].
  static const int _authRejectionsBeforeLongDelay = 3;

  void _onSubscriptionSucceeded(dynamic data) {
    cancelSubscribeRetry();
    _subscribeRetryBackoff.reset();
    _authRejections = 0;
  }

  void _onSubscriptionError(dynamic data) {
    options.log("SUBSCRIPTION_ERROR", name, "data: $data");

    final status = data is Map ? data["status"] : null;
    if (status == 401 || status == 403) {
      _authRejections++;
    } else {
      _authRejections = 0;
    }

    _subscribed = false;
    _scheduleSubscribeRetry();
  }

  /// Subscribes again after a backoff delay, as long as the connection that
  /// failed is still up and the channel still wanted. Transient failures (5xx,
  /// 429, network, auth headers) back off up to [PusherOptions.maxReconnectDelay];
  /// an auth refused with 401/403 several times in a row only every
  /// [PusherOptions.rejectedAuthRetryDelay].
  void _scheduleSubscribeRetry() {
    cancelSubscribeRetry();

    final socketId = client.socketId;
    // Without a connection, the next one subscribes again anyway.
    if (!client.connected || socketId == null) return;

    if (_subscribeRetrySocketId != socketId) {
      _subscribeRetrySocketId = socketId;
      _subscribeRetryBackoff.reset();
    }

    final Duration delay;
    if (_authRejections >= _authRejectionsBeforeLongDelay) {
      final longMs = options.rejectedAuthRetryDelay.inMilliseconds;
      delay = Duration(milliseconds: longMs ~/ 2 + _jitter.nextInt(longMs - longMs ~/ 2 + 1));
    } else {
      delay = _subscribeRetryBackoff.next(minAttempt: 1);
    }
    options.log("SUBSCRIBE_RETRY", name, "delay: ${delay.inMilliseconds}ms");

    _subscribeRetryTimer = Timer(delay, () {
      _subscribeRetryTimer = null;
      if (client.connected && client.socketId == socketId && !subscribed && isWanted) {
        subscribe(true);
      }
    });
  }

  static final _jitter = Random();

  /// The connection was closed: the next one subscribes again. Called by the
  /// client.
  void onConnectionClosed() {
    _subscribed = false;
    cancelSubscribeRetry();
  }

  /// A server `pusher:error` that names no channel arrived (see
  /// [PrivateChannel]). Called by the client.
  void onServerError(Map data) {}

  /// Cancels a pending subscription retry.
  @protected
  void cancelSubscribeRetry() {
    _subscribeRetryTimer?.cancel();
    _subscribeRetryTimer = null;
  }

  /// Whether the client still holds this channel (it wasn't unsubscribed).
  @protected
  bool get isWanted => identical(client.channelsCollection.get(name), this);
}
