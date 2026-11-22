import 'dart:convert';

import 'package:http/http.dart' as http;

import '../utils/auth_data.dart';
import '../utils/member.dart';
import 'channel.dart';

/// Represents a private channel in Pusher.
class PrivateChannel extends Channel {
  PrivateChannel({required super.client, required super.name}) {
    onSubscriptionSuccess(_onSubscriptionSuccess);
    onSubscriptionError(_onSubscriptionError);
    onSubscriptionCount(_onSubscriptionCount);
  }

  String? userId;

  /// The user member of the channel.
  Member? get member => userId != null ? Member(id: userId!) : null;

  AuthData? authData;

  /// The connection (socket id) a subscription is pending for: authorizing,
  /// or waiting for the server's answer. Another subscribe for the same
  /// connection is skipped, so a reconnect authorizes each channel once.
  String? _pendingFor;

  /// Subscribes to the private channel.
  ///
  /// A failed auth request (an HTTP error, a network error, an exception from
  /// the auth headers) emits `pusher:error` and `pusher:subscription_error`,
  /// and is retried with a backoff while the connection lasts.
  @override
  void subscribe([bool force = false]) async {
    final socketId = client.socketId;
    if (!client.connected || (subscribed && !force) || socketId == null) {
      return;
    }
    if (_pendingFor == socketId) return;

    cancelSubscribeRetry();
    subscribed = false;
    _pendingFor = socketId;

    options.log("SUBSCRIBE", name);

    final String? failure;
    try {
      failure = await _authorizeAndSubscribe(socketId);
    } catch (e) {
      _failSubscription(socketId, "Unable to authenticate channel $name: $e");
      return;
    }
    if (failure != null) _failSubscription(socketId, failure);
  }

  /// Returns why it failed, or null once the subscribe was sent (or is moot:
  /// the connection changed or the channel was unsubscribed meanwhile).
  Future<String?> _authorizeAndSubscribe(String socketId) async {
    final payload = {"channel_name": name, "socket_id": socketId};

    final authOptionsHeaders = authOptions.headersAsync != null ? await authOptions.headersAsync!() : authOptions.headers;

    final response = await http.post(Uri.parse(authOptions.endpoint), body: payload, headers: authOptionsHeaders);

    options.log("AUTH_RESPONSE", name, "options: $authOptions\n  payload: $payload\n  response: ${response.body}");

    if (response.statusCode != 200) {
      return "Unable to authenticate channel $name, status code: ${response.statusCode}";
    }

    final data = jsonDecode(response.body);
    if (data is! Map) {
      return "Invalid auth response data [$data], excepted Map got ${data.runtimeType}";
    } else if (!data.containsKey("auth")) {
      return "Invalid auth response data [$data], auth key is missing";
    }

    if (client.socketId != socketId || !client.connected || !isWanted) {
      if (_pendingFor == socketId) _pendingFor = null;
      return null;
    }

    authData = AuthData.fromJson(data);
    userId = authData!.channelData?.userId;
    client.sendEvent("pusher:subscribe", {"channel": name, "auth": authData!.auth, "channel_data": authData!.channelData?.toJsonString()});
    return null;
  }

  void _failSubscription(String socketId, String message) {
    if (_pendingFor == socketId) _pendingFor = null;
    // The connection changed meanwhile: the new one subscribes again.
    if (client.socketId != socketId) return;

    handleEvent("pusher:error", message);
    handleEvent("pusher:subscription_error", {"type": "AuthError", "error": message});
  }

  @override
  void unsubscribe() {
    _pendingFor = null;
    super.unsubscribe();
  }

  void _onSubscriptionSuccess(data) {
    options.log("SUBSCRIPTION_SUCCESS", name, "data: $data");

    _pendingFor = null;
    subscribed = true;
  }

  void _onSubscriptionError(data) {
    _pendingFor = null;
  }

  int _subscriptionCount = 0;

  /// The number of subscriptions to the channel.
  int get subscriptionCount => _subscriptionCount;

  void _onSubscriptionCount(data) {
    options.log("SUBSCRIPTION_COUNT", name, "data: $data");

    _subscriptionCount = data["subscription_count"];
  }

  /// Binding for the subscription count event.
  void onSubscriptionCount(Function listener) => bind("pusher:subscription_count", listener);

  /// Send an event to the channel.
  void trigger(String event, [data]) {
    options.log("TRIGGER", name, "event: $event\n  data: $data");

    if (!event.startsWith("client-")) {
      event = "client-$event";
    }

    client.sendEvent(event, data, name);
  }
}
