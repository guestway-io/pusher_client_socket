library pusher_client_socket;

import 'dart:async';
import 'dart:convert';

import 'package:meta/meta.dart';
import 'package:pusher_client_socket/channels/channel.dart';
import 'package:pusher_client_socket/src/channels.collection.dart';
import 'package:pusher_client_socket/src/events_listeners.collection.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:web_socket_client/web_socket_client.dart' show ConnectionState, Connecting, Connected, Reconnecting, Reconnected, Disconnecting, Disconnected;

import 'src/options.dart';
import 'src/reconnect.dart';

export 'package:web_socket_client/src/connection_state.dart';

export 'channels/channel.dart';
export 'src/options.dart';

/// Represents a client for connecting to a Pusher server.
///
/// Once [connect]ed, the client keeps its connection up until [disconnect] is
/// called: it reconnects after a connection error, a lost connection, any close
/// code but 4000-4099 (which the Pusher protocol reserves for connections that
/// must not be retried) and its own activity timeout, with a jittered
/// exponential backoff ([PusherOptions.minReconnectDelay],
/// [PusherOptions.maxReconnectDelay]) that never gives up, and re-subscribes
/// its channels once connected again.
///
/// The client owns every connection attempt: it dials the WebSocket itself
/// (web_socket_channel) and never has more than one connection open.
class PusherClient {
  /// The options of the client.
  final PusherOptions options;

  PusherClient({
    required this.options,
    @visibleForTesting WebSocketChannel Function(Uri uri)? channelFactory,
    @visibleForTesting this.healthyConnectionAfter = const Duration(seconds: 10),
  }) : _channelFactory = channelFactory ?? WebSocketChannel.connect {
    onConnectionEstablished(_onConnectionEstablished);
    bind("pusher:ping", _onPing);
    bind("pusher:pong", (data) => _resetActivityCheck());

    if (options.autoConnect) connect();
  }

  final WebSocketChannel Function(Uri uri) _channelFactory;

  /// How long a connection must stay up after `pusher:connection_established`
  /// before the backoff starts over. A connection the server accepts and then
  /// closes right away counts as a failed attempt, so it can't make the client
  /// reconnect in a tight loop.
  @visibleForTesting
  final Duration healthyConnectionAfter;

  /// The current connection, from the moment it is dialled.
  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _channelSubscription;

  /// Bumped whenever the current connection is replaced or closed: what an
  /// older connection does after that is ignored (and a connection that
  /// completes after it was abandoned is closed right away).
  int _generation = 0;

  /// Whether the client should be connected: set by [connect], cleared by
  /// [disconnect] and by a close code that forbids reconnecting.
  bool _keepConnected = false;

  /// Whether the client lost a connection since [connect] (reports
  /// [Reconnecting] and [Reconnected] instead of [Connecting] and [Connected]).
  bool _reconnecting = false;

  Timer? _reconnectTimer;

  Timer? _healthyTimer;

  int _reconnectionAttempts = 0;

  late final ReconnectBackoff _backoff = ReconnectBackoff(
    minDelay: options.minReconnectDelay,
    maxDelay: options.maxReconnectDelay,
  );

  String? _socketId;

  String? get socketId => _socketId;

  bool _connected = false;

  /// Returns a boolean value indicating whether the client is connected.
  bool get connected => _connected;

  ConnectionState _connectionState = const Disconnected();

  /// Returns the current connection state of the client.
  ConnectionState get connectionState => _connectionState;

  /// Connects the client to the server, replacing the current connection if any.
  void connect() {
    _keepConnected = true;
    _reconnecting = false;
    _cancelReconnect();
    _reconnectionAttempts = 0;
    _backoff.reset();
    _closeChannel();
    _dial();
  }

  /// Disconnects the client from the server. The client doesn't reconnect
  /// until [connect] is called again.
  void disconnect([int? code, String? reason]) {
    options.log("DISCONNECT", null, "code: $code\n  reason: $reason");

    _keepConnected = false;
    _cancelReconnect();
    _closeChannel(code, reason);
    _setState(Disconnected(code: code, reason: reason));
  }

  Future<void> _dial() async {
    final generation = ++_generation;
    _setState(_reconnecting ? const Reconnecting() : const Connecting());

    final WebSocketChannel channel;
    try {
      channel = _channelFactory(options.uri);
    } catch (error, stackTrace) {
      _onConnectionLost(Disconnected(error: error, stackTrace: stackTrace));
      return;
    }
    _channel = channel;

    try {
      await channel.ready.timeout(options.connectTimeout);
    } catch (error, stackTrace) {
      // Closing the sink also closes the connection if it still completes.
      _closeSink(channel);
      if (generation == _generation) {
        _onConnectionLost(Disconnected(error: error, stackTrace: stackTrace));
      }
      return;
    }

    if (generation != _generation) {
      // Abandoned while dialling (disconnect(), connect()): don't keep it open.
      _closeSink(channel);
      return;
    }

    _channelSubscription = channel.stream.listen(
      (message) {
        if (generation == _generation) _onMessageReceived(message);
      },
      onError: (error) {
        if (generation == _generation) _onEvent('error', error);
      },
      onDone: () {
        if (generation == _generation) {
          _onConnectionLost(Disconnected(code: channel.closeCode, reason: channel.closeReason));
        }
      },
    );

    _setState(_reconnecting ? const Reconnected() : const Connected());
  }

  /// Closes the current connection, if any, and ignores everything it does
  /// after.
  void _closeChannel([int? code, String? reason]) {
    _generation++;

    final channel = _channel;
    _channel = null;
    _channelSubscription?.cancel();
    _channelSubscription = null;

    _stopActivityTimer();
    _healthyTimer?.cancel();
    _healthyTimer = null;
    _connected = false;
    _socketId = null;

    if (channel != null) _closeSink(channel, code, reason);
  }

  static void _closeSink(WebSocketChannel channel, [int? code, String? reason]) {
    // A client may only send 1000 or 3000-4999; anything else would throw
    // where nobody can catch it.
    final validCode = code != null && (code == 1000 || (code >= 3000 && code <= 4999));
    channel.sink.close(validCode ? code : null, validCode ? reason : null).catchError((_) {});
  }

  void _setState(ConnectionState state) {
    if (state == _connectionState) return;

    options.log("CONNECTION_STATE_CHANGED", null, "the connection state changed from ${_stateName(_connectionState)} to ${_stateName(state)}");
    _connectionState = state;

    /// Check if the client is connected (fixed by @usmanabdulmajid)
    _connected = state is Connected || state is Reconnected;

    _onEvent("connection_state_changed", state);

    switch (state) {
      case Connecting():
        _onEvent('connecting', null);
      case Connected():
        _onEvent('connected', null);
        _resetActivityCheck();
      case Reconnecting():
        _onEvent('reconnecting', null);
      case Reconnected():
        _onEvent('reconnected', null);
        _resetActivityCheck();
      case Disconnecting():
        _onEvent('disconnecting', null);
      case Disconnected():
        _stopActivityTimer();
        _socketId = null;
        _onEvent('disconnected', state);
        if (state.error != null) {
          _onEvent("connection_error", state.error);
        }
    }
  }

  static String _stateName(ConnectionState state) => switch (state) {
        Connecting() => 'CONNECTING',
        Connected() => 'CONNECTED',
        Reconnecting() => 'RECONNECTING',
        Reconnected() => 'RECONNECTED',
        Disconnecting() => 'DISCONNECTING',
        Disconnected() => 'DISCONNECTED',
      };

  /// The current connection was lost, or couldn't be established.
  void _onConnectionLost(Disconnected state) {
    _closeChannel();
    _setState(state);

    if (!_keepConnected) return;

    final action = closeActionFor(state.code);
    if (action == PusherCloseAction.doNotReconnect) {
      options.log("CONNECTION_REFUSED", null, "code: ${state.code}\n  reason: ${state.reason}\n  not reconnecting");
      _keepConnected = false;
      return;
    }

    _scheduleReconnect(action);
  }

  /// Dials again after a backoff delay.
  void _scheduleReconnect(PusherCloseAction action) {
    _cancelReconnect();

    final maxAttempts = options.maxReconnectionAttempts;
    if (maxAttempts != null && _reconnectionAttempts >= maxAttempts) {
      _keepConnected = false;
      _setState(const Disconnected(reason: "Max reconnection attempts reached"));
      return;
    }

    _reconnectionAttempts++;
    final delay = _backoff.next(minAttempt: action == PusherCloseAction.reconnectWithBackoff ? 1 : 0);
    options.log("RECONNECT", null, "attempt: $_reconnectionAttempts\n  delay: ${delay.inMilliseconds}ms");

    _reconnecting = true;
    _setState(const Reconnecting());
    _reconnectTimer = Timer(delay, () {
      _reconnectTimer = null;
      if (_keepConnected) _dial();
    });
  }

  void _cancelReconnect() {
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
  }

  void _onConnectionEstablished(Map data) {
    options.log("CONNECTION_ESTABLISHED", null, "data: $data");
    _socketId = data['socket_id'];
    _connected = true;

    _healthyTimer?.cancel();
    _healthyTimer = Timer(healthyConnectionAfter, () {
      _healthyTimer = null;
      _reconnectionAttempts = 0;
      _backoff.reset();
    });

    _reSubscribe();
  }

  void _onPing(data) {
    options.log("PINGING", null, "data: $data");
    sendEvent("pusher:pong", data);
  }

  Timer? _activityTimer;

  void _sendActivityCheck() {
    _stopActivityTimer();

    sendEvent("pusher:ping", null);

    _activityTimer = Timer(Duration(milliseconds: options.pongTimeout), _onActivityTimeout);
  }

  /// The server didn't answer our ping: the connection is dead, even if the
  /// socket doesn't know it yet. Drop it and connect again.
  void _onActivityTimeout() {
    _onEvent("pusher:error", "Activity timeout");
    _onConnectionLost(const Disconnected(reason: "Activity timeout"));
  }

  void _resetActivityCheck() {
    _stopActivityTimer();
    _activityTimer = Timer(Duration(milliseconds: options.activityTimeout), _sendActivityCheck);
  }

  void _stopActivityTimer() {
    _activityTimer?.cancel();
    _activityTimer = null;
  }

  final _eventsListeners = EventsListenersCollection();

  void _onMessageReceived(message) {
    options.log("MESSAGE_RECEIVED", null, "$message");

    dynamic event;

    try {
      event = jsonDecode(message);
    } catch (e) {
      throw Exception('Invalid message "$message", cannot decode message json');
    }

    if (event is Map) {
      if (event.containsKey('event')) {
        String eventName = event['event'].replaceAll('pusher_internal:', 'pusher:');
        dynamic data = event['data'];

        if (data is String) {
          try {
            data = jsonDecode(data);
          } catch (e) {}
        }

        _onEvent(eventName, data, event["channel"]);
      } else {
        throw Exception('Invalid event "$event", messing event name');
      }
    } else {
      throw Exception('Invalid event "$event", the event must be map but ${event.runtimeType} given');
    }
  }

  void _onEvent(String event, data, [String? channel]) {
    _eventsListeners.handleEvent(event, data, channel);

    if (channel != null) {
      channelsCollection.get(channel)?.handleEvent(event, data);
    }
  }

  /// Binds a listener to an event.
  void bind(String event, Function listener) {
    options.log("EVENT_BINDING", "event: $event");

    _eventsListeners.bind(event, listener);
  }

  /// Send an event to the server. Dropped while the client has no connection.
  void sendEvent(String event, [dynamic data, String? channel]) {
    options.log("SEND_EVENT", null, "event: $event\n  data: $data");
    if (!_connected) return;
    _channel?.sink.add(jsonEncode({"event": event, "data": data, if (channel != null) "channel": channel}));
  }

  /// Binding to the connection state change event.
  void onConnectionStateChange(Function(ConnectionState) listener) => bind("connection_state_changed", listener);

  /// Binding to the connection state change event.
  void onConnecting(Function listener) => bind('connecting', listener);

  /// Binding to the connection state change event.
  void onConnected(Function listener) => bind('connected', listener);

  /// Binding to the connection state change event.
  void onConnectionEstablished(Function listener) => bind("pusher:connection_established", listener);

  /// Binding to the connection state change event.
  void onReconnecting(Function listener) => bind('reconnecting', listener);

  /// Binding to the connection state change event.
  void onReconnected(Function listener) => bind('reconnected', listener);

  /// Binding to the connection state change event.
  void onDisconnecting(Function listener) => bind('disconnecting', listener);

  /// Binding to the connection state change event.
  void onDisconnected(Function listener) => bind('disconnected', listener);

  /// Binding to the connection state change event.
  void onConnectionError(Function(dynamic error) listener) => bind('connection_error', listener);

  /// Binding to the connection state change event.
  void onError(Function(dynamic error) listener) => bind('pusher:error', listener);

  @internal
  late final channelsCollection = ChannelsCollection(this);

  /// Returns a channel by name.
  T channel<T extends Channel>(String channelName, {bool subscribe = false}) => channelsCollection.channel<T>(channelName, subscribe: subscribe);

  /// Returns a private channel by name.
  PrivateChannel private(String channelName, {bool subscribe = false}) => channel(channelName.startsWith("private-") ? channelName : "private-$channelName", subscribe: subscribe);

  /// Returns a private encrypted channel by name.
  PrivateChannel privateEncrypted(String channelName, {bool subscribe = false}) =>
      channel(channelName.startsWith("private-encrypted-") ? channelName : "private-encrypted-$channelName", subscribe: subscribe);

  /// Returns a presence channel by name.
  PresenceChannel presence(String channelName, {bool subscribe = false}) =>
      channel(channelName.startsWith("presence-") ? channelName : "presence-$channelName", subscribe: subscribe);

  /// Subscribes to a channel by name.
  T subscribe<T extends Channel>(String channelName) => channel<T>(channelName, subscribe: true);

  /// Unsubscribes from a channel by name.
  void unsubscribe(String channelName) => channel(channelName).unsubscribe();

  /// Unsubscribes from all channels and clears the collection.
  void unsubscribeAllClearChannels() => channelsCollection.clear();

  /// Re-subscribes to all channels.
  void _reSubscribe() => channelsCollection.forEach((channel) => channel.subscribe(true));
}
