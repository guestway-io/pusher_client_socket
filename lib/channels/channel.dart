import 'dart:async';

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

  void _onSubscriptionSucceeded(dynamic data) {
    cancelSubscribeRetry();
    _subscribeRetryBackoff.reset();
  }

  void _onSubscriptionError(dynamic data) {
    options.log("SUBSCRIPTION_ERROR", name, "data: $data");

    _subscribed = false;
    _scheduleSubscribeRetry();
  }

  /// Subscribes again after a backoff delay, as long as the connection that
  /// failed is still up and the channel still wanted.
  void _scheduleSubscribeRetry() {
    cancelSubscribeRetry();

    final socketId = client.socketId;
    // Without a connection, the next one subscribes again anyway.
    if (!client.connected || socketId == null) return;

    final delay = _subscribeRetryBackoff.next(minAttempt: 1);
    options.log("SUBSCRIBE_RETRY", name, "delay: ${delay.inMilliseconds}ms");

    _subscribeRetryTimer = Timer(delay, () {
      _subscribeRetryTimer = null;
      if (client.connected && client.socketId == socketId && !subscribed && isWanted) {
        subscribe(true);
      }
    });
  }

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
