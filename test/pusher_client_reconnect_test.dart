import 'dart:convert';
import 'dart:io' as io;

import 'package:flutter_test/flutter_test.dart';
import 'package:pusher_client_socket/pusher_client_socket.dart';
import 'package:web_socket_client/web_socket_client.dart' show Backoff, WebSocket;

/// A minimal Pusher server on localhost.
class FakePusherServer {
  late final io.HttpServer _http;

  /// Every WebSocket upgrade request, accepted or not.
  int upgradeRequests = 0;

  /// Answers upgrade requests with this HTTP status instead of accepting them
  /// (a 502 from the tunnel, a 503 from a draining server).
  int? rejectWithStatus;

  /// Closes every new connection right away with this code.
  int? refuseWithCloseCode;

  bool answerPings = true;

  final List<io.WebSocket> sockets = [];
  final List<Map<String, dynamic>> received = [];

  int get established => sockets.length;

  String get host => 'ws://127.0.0.1:${_http.port}';

  Future<void> start() async {
    _http = await io.HttpServer.bind(io.InternetAddress.loopbackIPv4, 0);
    _http.listen((request) async {
      upgradeRequests++;
      final status = rejectWithStatus;
      if (status != null) {
        request.response.statusCode = status;
        await request.response.close();
        return;
      }

      final socket = await io.WebSocketTransformer.upgrade(request);
      final refuse = refuseWithCloseCode;
      if (refuse != null) {
        await socket.close(refuse, 'refused');
        return;
      }

      sockets.add(socket);
      socket.add(jsonEncode({
        'event': 'pusher:connection_established',
        'data': jsonEncode({'socket_id': '1.${sockets.length}', 'activity_timeout': 120}),
      }));
      socket.listen((message) {
        final event = jsonDecode(message as String) as Map<String, dynamic>;
        received.add(event);
        if (event['event'] == 'pusher:ping' && answerPings) {
          try {
            socket.add(jsonEncode({'event': 'pusher:pong', 'data': '{}'}));
          } on StateError {
            // The client closed this connection meanwhile.
          }
        }
      }, onError: (_) {});
    });
  }

  /// Closes the newest connection from the server side.
  Future<void> closeLatest([int? code, String? reason]) => sockets.last.close(code, reason);

  int subscribes(String channel) => received.where((e) => e['event'] == 'pusher:subscribe' && e['data']['channel'] == channel).length;

  Future<void> stop() async {
    for (final socket in sockets) {
      await socket.close();
    }
    await _http.close(force: true);
  }
}

Future<void> eventually(bool Function() condition, {Duration timeout = const Duration(seconds: 10)}) async {
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) fail('condition not met within $timeout');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void main() {
  late FakePusherServer server;
  late PusherClient client;
  late List<String> events;

  PusherClient makeClient({
    int activityTimeout = 120000,
    int pongTimeout = 30000,
    int? maxReconnectionAttempts,
    Duration minReconnectDelay = const Duration(milliseconds: 10),
    WebSocket Function(Uri uri, Backoff backoff)? webSocketFactory,
  }) {
    final c = PusherClient(
      options: PusherOptions(
        key: 'app-key',
        host: server.host,
        authOptions: const PusherAuthOptions('http://127.0.0.1/unused'),
        autoConnect: false,
        activityTimeout: activityTimeout,
        pongTimeout: pongTimeout,
        maxReconnectionAttempts: maxReconnectionAttempts,
        minReconnectDelay: minReconnectDelay,
        maxReconnectDelay: minReconnectDelay * 5,
      ),
      webSocketFactory: webSocketFactory,
    );
    c.onDisconnected((state) => events.add('disconnected ${state is Disconnected ? state.code : null}'));
    c.onConnectionError((error) => events.add('connection_error'));
    c.onError((error) => events.add('error $error'));
    return c;
  }

  setUp(() async {
    events = [];
    server = FakePusherServer();
    await server.start();
  });

  tearDown(() async {
    client.disconnect();
    await server.stop();
  });

  test('reconnects and re-subscribes after the server closes with 4200 (deploy)', () async {
    client = makeClient()..connect();
    await eventually(() => client.socketId != null);
    client.subscribe('inbox');
    await eventually(() => server.subscribes('inbox') == 1);

    await server.closeLatest(4200, 'shutting down');

    await eventually(() => server.established == 2 && server.subscribes('inbox') == 2);
    expect(client.connected, isTrue);
    expect(events, contains('disconnected 4200'));
  });

  for (final code in [1000, 1001, 4100, 4201, 4300]) {
    test('reconnects after close code $code', () async {
      client = makeClient()..connect();
      await eventually(() => client.socketId != null);

      await server.closeLatest(code, 'bye');

      await eventually(() => server.established >= 2 && client.socketId != null);
      expect(client.connected, isTrue);
    });
  }

  test('reconnects after a connection that dropped without a close code', () async {
    client = makeClient()..connect();
    await eventually(() => client.socketId != null);

    await server.closeLatest();

    await eventually(() => server.established == 2 && client.socketId != null);
  });

  for (final code in [4000, 4001, 4099]) {
    test('does not reconnect after close code $code', () async {
      client = makeClient()..connect();
      await eventually(() => client.socketId != null);

      // The server refuses this client from now on.
      server.refuseWithCloseCode = code;
      await server.closeLatest(code, 'refused');

      await eventually(() => client.connectionState is Disconnected && (client.connectionState as Disconnected).code == code);
      // web_socket_client may already have re-dialled once before the client
      // saw the close code; after that, nothing.
      await Future<void>.delayed(const Duration(milliseconds: 100));
      final requests = server.upgradeRequests;
      await Future<void>.delayed(const Duration(milliseconds: 500));
      expect(server.upgradeRequests, requests);
      expect(server.upgradeRequests, lessThanOrEqualTo(2));
      expect(client.connected, isFalse);
      expect(client.connectionState, isA<Disconnected>());
    });
  }

  for (final status in [502, 503]) {
    test('keeps reconnecting while the server answers HTTP $status, then connects', () async {
      server.rejectWithStatus = status;
      client = makeClient()..connect();

      await eventually(() => server.upgradeRequests >= 5);
      expect(events, contains('connection_error'));
      server.rejectWithStatus = null;

      await eventually(() => client.socketId != null);
      expect(client.connected, isTrue);
    });
  }

  test('reconnects through HTTP 503s after an established connection drops', () async {
    client = makeClient()..connect();
    await eventually(() => client.socketId != null);
    client.subscribe('inbox');
    await eventually(() => server.subscribes('inbox') == 1);

    server.rejectWithStatus = 503;
    await server.closeLatest(4200, 'shutting down');
    final requests = server.upgradeRequests;
    await eventually(() => server.upgradeRequests >= requests + 5);
    server.rejectWithStatus = null;

    await eventually(() => server.established == 2 && server.subscribes('inbox') == 2);
  });

  test('re-creates the socket once web_socket_client gives up', () async {
    var sockets = 0;
    server.rejectWithStatus = 502;
    client = makeClient(
      // web_socket_client gives up once its backoff delays add up to its
      // timeout (60 s by default); make that quick.
      webSocketFactory: (uri, backoff) {
        sockets++;
        return WebSocket(uri, backoff: backoff, timeout: const Duration(milliseconds: 100));
      },
    )..connect();

    await eventually(() => sockets >= 3);
    server.rejectWithStatus = null;

    await eventually(() => client.socketId != null);
    expect(client.connected, isTrue);
  });

  test('reconnects after its own activity timeout', () async {
    server.answerPings = false;
    client = makeClient(activityTimeout: 100, pongTimeout: 100)..connect();
    await eventually(() => client.socketId != null);
    client.subscribe('inbox');
    await eventually(() => server.subscribes('inbox') == 1);

    await eventually(() => server.established >= 2 && server.subscribes('inbox') >= 2);
    server.answerPings = true;
    expect(events, contains('error Activity timeout'));
    expect(events, contains('disconnected null'));
  });

  test('stays connected while the server answers pings', () async {
    client = makeClient(activityTimeout: 50, pongTimeout: 100)..connect();
    await eventually(() => client.socketId != null);

    await Future<void>.delayed(const Duration(milliseconds: 500));
    expect(server.received.where((e) => e['event'] == 'pusher:ping').length, greaterThan(2));
    expect(server.established, 1);
    expect(client.connected, isTrue);
  });

  test('disconnect() stops reconnecting', () async {
    server.rejectWithStatus = 502;
    client = makeClient()..connect();
    await eventually(() => server.upgradeRequests >= 3);

    client.disconnect();
    await Future<void>.delayed(const Duration(milliseconds: 100));
    final requests = server.upgradeRequests;
    await Future<void>.delayed(const Duration(milliseconds: 500));

    expect(server.upgradeRequests, requests);
    expect(client.connectionState, isA<Disconnected>());
  });

  test('disconnect() cancels a scheduled reconnect', () async {
    server.answerPings = false;
    client = makeClient(activityTimeout: 50, pongTimeout: 50, minReconnectDelay: const Duration(seconds: 2))..connect();
    await eventually(() => client.connectionState is Reconnecting);

    client.disconnect();
    final established = server.established;
    await Future<void>.delayed(const Duration(milliseconds: 500));

    expect(server.established, established);
    expect(client.connectionState, isA<Disconnected>());
  });

  test('disconnect() and sendEvent() without a connection do not throw', () {
    client = makeClient();
    client.disconnect();
    client.sendEvent('client-event', {'a': 1});
    client.unsubscribe('inbox');
    expect(client.connectionState, isA<Disconnected>());
  });

  test('gives up after maxReconnectionAttempts when set', () async {
    server.rejectWithStatus = 502;
    var sockets = 0;
    client = makeClient(
      maxReconnectionAttempts: 2,
      webSocketFactory: (uri, backoff) {
        sockets++;
        return WebSocket(uri, backoff: backoff, timeout: const Duration(milliseconds: 50));
      },
    )..connect();

    await eventually(() => client.connectionState == const Disconnected(reason: 'Max reconnection attempts reached'));
    expect(sockets, 3);
  });
}
