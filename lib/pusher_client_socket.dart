library pusher_client_socket;

import 'dart:async';
import 'dart:convert';

import 'package:meta/meta.dart';
import 'package:pusher_client_socket/channels/channel.dart';
import 'package:pusher_client_socket/src/channels.collection.dart';
import 'package:pusher_client_socket/src/events_listeners.collection.dart';
import 'package:web_socket_client/web_socket_client.dart';

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
/// must not be retried) and its own activity timeout, with a capped, jittered
/// backoff ([PusherOptions.minReconnectDelay], [PusherOptions.maxReconnectDelay]),
/// and re-subscribes its channels once connected again.
class PusherClient {
  /// The options of the client.
  final PusherOptions options;

  PusherClient({
    required this.options,
    @visibleForTesting WebSocket Function(Uri uri, Backoff backoff)? webSocketFactory,
  }) : _webSocketFactory = webSocketFactory ?? _defaultWebSocketFactory {
    onConnectionEstablished(_onConnectionEstablished);
    bind("pusher:ping", _onPing);
    bind("pusher:pong", (data) => _resetActivityCheck());

    if (options.autoConnect) connect();
  }

  static WebSocket _defaultWebSocketFactory(Uri uri, Backoff backoff) => WebSocket(uri, backoff: backoff);

  final WebSocket Function(Uri uri, Backoff backoff) _webSocketFactory;

  WebSocket? __socket;
  StreamSubscription<ConnectionState>? _connectionSubscription;
  StreamSubscription<dynamic>? _messagesSubscription;

  /// Whether the client should be connected: set by [connect], cleared by
  /// [disconnect] and by a close code that forbids reconnecting.
  bool _keepConnected = false;

  Timer? _reconnectTimer;

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
    _cancelReconnect();
    _reconnectionAttempts = 0;
    _backoff.restart();
    _openSocket();
  }

  /// Disconnects the client from the server. The client doesn't reconnect
  /// until [connect] is called again.
  void disconnect([int? code, String? reason]) {
    options.log("DISCONNECT", null, "code: $code\n  reason: $reason");

    _keepConnected = false;
    _cancelReconnect();
    _closeSocket(code, reason);
    _setState(Disconnected(code: code, reason: reason));
  }

  void _openSocket() {
    _closeSocket();

    final socket = _webSocketFactory(options.uri, _backoff);
    __socket = socket;
    _connectionSubscription = socket.connection.listen(_onConnectionStateChange, onDone: _onSocketClosed);
    _messagesSubscription = socket.messages.listen(_onMessageReceived, onError: (err) => _onEvent('error', err));
  }

  /// Closes the current socket, if any, and ignores everything it does after.
  void _closeSocket([int? code, String? reason]) {
    final socket = __socket;
    __socket = null;

    _connectionSubscription?.cancel();
    _connectionSubscription = null;
    _messagesSubscription?.cancel();
    _messagesSubscription = null;

    _stopActivityTimer();
    _connected = false;
    _socketId = null;

    socket?.close(code, reason);
  }

  void _onConnectionStateChange(ConnectionState state) {
    _setState(state);

    if (state is Disconnected) _onConnectionLost(state);
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

  /// The current socket lost its connection, or couldn't connect.
  void _onConnectionLost(Disconnected state) {
    if (!_keepConnected) return;

    switch (closeActionFor(state.code)) {
      case PusherCloseAction.doNotReconnect:
        options.log("CONNECTION_REFUSED", null, "code: ${state.code}\n  reason: ${state.reason}\n  not reconnecting");
        _keepConnected = false;
        _closeSocket();
      case PusherCloseAction.reconnectWithBackoff:
        // web_socket_client would re-dial right away, again and again if the
        // server keeps closing the connection: back off instead.
        _closeSocket();
        _scheduleReconnect();
      case PusherCloseAction.reconnect:
        // web_socket_client is already reconnecting (right away, then with
        // [_backoff]). If it gives up, [_onSocketClosed] takes over.
        break;
    }
  }

  /// web_socket_client closed the current socket for good: it gave up
  /// reconnecting (after about a minute of failed attempts).
  void _onSocketClosed() {
    _closeSocket();

    if (_keepConnected) _scheduleReconnect();
  }

  /// Re-creates the socket after a backoff delay.
  void _scheduleReconnect() {
    _cancelReconnect();

    final maxAttempts = options.maxReconnectionAttempts;
    if (maxAttempts != null && _reconnectionAttempts >= maxAttempts) {
      _keepConnected = false;
      _setState(const Disconnected(reason: "Max reconnection attempts reached"));
      return;
    }

    _reconnectionAttempts++;
    final delay = _backoff.next();
    options.log("RECONNECT", null, "attempt: $_reconnectionAttempts\n  delay: ${delay.inMilliseconds}ms");

    _setState(const Reconnecting());
    _reconnectTimer = Timer(delay, () {
      _reconnectTimer = null;
      if (_keepConnected) _openSocket();
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
    _reconnectionAttempts = 0;
    _backoff.restart();
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

    _closeSocket();
    _setState(const Disconnected(reason: "Activity timeout"));

    if (_keepConnected) _scheduleReconnect();
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

  /// Send an event to the server. Dropped while the client has no connection,
  /// like web_socket_client drops it while it is reconnecting.
  void sendEvent(String event, [dynamic data, String? channel]) {
    options.log("SEND_EVENT", null, "event: $event\n  data: $data");
    __socket?.send(jsonEncode({"event": event, "data": data, if (channel != null) "channel": channel}));
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
