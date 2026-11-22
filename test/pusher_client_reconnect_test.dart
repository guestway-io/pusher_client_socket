import 'dart:convert';
import 'dart:io' as io;

import 'package:flutter_test/flutter_test.dart';
import 'package:pusher_client_socket/pusher_client_socket.dart';

/// A minimal Pusher server (WebSocket + channel auth endpoint) on localhost.
class FakePusherServer {
  late final io.HttpServer _http;

  /// Every WebSocket upgrade request, accepted or not.
  int upgradeRequests = 0;

  /// When each upgrade request arrived.
  final List<DateTime> upgradeTimes = [];

  /// Connections currently open on the server side.
  int open = 0;

  /// Answers upgrade requests with this HTTP status instead of accepting them
  /// (a 502 from the tunnel, a 503 from a draining server).
  int? rejectWithStatus;

  /// Waits this long before accepting an upgrade (a slow server).
  Duration acceptDelay = Duration.zero;

  /// Accepts new connections, then closes them right away: with this code, or
  /// with no code when [closeAfterAcceptWithoutCode] is set.
  int? closeAfterAccept;
  bool closeAfterAcceptWithoutCode = false;

  /// Sends `pusher:connection_established` before closing such a connection.
  bool establishBeforeClosing = false;

  bool answerPings = true;

  /// Fails this many auth requests with HTTP [authFailureStatus].
  int authFailuresLeft = 0;
  int authFailureStatus = 503;
  int authRequests = 0;

  /// Answers this many `pusher:subscribe` with `pusher:subscription_error`.
  int subscriptionErrorsLeft = 0;

  final List<io.WebSocket> sockets = [];
  final List<Map<String, dynamic>> received = [];

  int get established => sockets.length;

  String get host => 'ws://127.0.0.1:${_http.port}';

  String get authEndpoint => 'http://127.0.0.1:${_http.port}/auth';

  Future<void> start() async {
    _http = await io.HttpServer.bind(io.InternetAddress.loopbackIPv4, 0);
    _http.listen((request) async {
      if (request.uri.path == '/auth') {
        authRequests++;
        await utf8.decoder.bind(request).join();
        if (authFailuresLeft > 0) {
          authFailuresLeft--;
          request.response.statusCode = authFailureStatus;
        } else {
          request.response.headers.contentType = io.ContentType.json;
          request.response.write(jsonEncode({'auth': 'app-key:signature'}));
        }
        await request.response.close();
        return;
      }

      upgradeRequests++;
      upgradeTimes.add(DateTime.now());
      final status = rejectWithStatus;
      if (status != null) {
        request.response.statusCode = status;
        await request.response.close();
        return;
      }
      if (acceptDelay > Duration.zero) await Future<void>.delayed(acceptDelay);

      final socket = await io.WebSocketTransformer.upgrade(request);
      open++;
      socket.done.then((_) => open--);

      final closeCode = closeAfterAccept;
      if (closeCode != null || closeAfterAcceptWithoutCode) {
        if (establishBeforeClosing) _establish(socket, 0);
        socket.listen((_) {}, onError: (_) {});
        await socket.close(closeCode);
        return;
      }

      sockets.add(socket);
      _establish(socket, sockets.length);
      socket.listen((message) {
        final event = jsonDecode(message as String) as Map<String, dynamic>;
        received.add(event);
        if (event['event'] == 'pusher:ping' && answerPings) {
          _send(socket, {'event': 'pusher:pong', 'data': '{}'});
        }
        if (event['event'] == 'pusher:subscribe') {
          final channel = event['data']['channel'];
          if (subscriptionErrorsLeft > 0) {
            subscriptionErrorsLeft--;
            _send(socket, {'event': 'pusher:subscription_error', 'channel': channel, 'data': jsonEncode({'type': 'AuthError', 'status': 401})});
          } else {
            _send(socket, {'event': 'pusher_internal:subscription_succeeded', 'channel': channel, 'data': '{}'});
          }
        }
      }, onError: (_) {});
    });
  }

  void _establish(io.WebSocket socket, int n) => _send(socket, {
        'event': 'pusher:connection_established',
        'data': jsonEncode({'socket_id': '1.$n', 'activity_timeout': 120}),
      });

  void _send(io.WebSocket socket, Map<String, dynamic> event) {
    try {
      socket.add(jsonEncode(event));
    } on StateError {
      // The client closed this connection meanwhile.
    }
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

Future<void> wait(int milliseconds) => Future<void>.delayed(Duration(milliseconds: milliseconds));

void main() {
  late FakePusherServer server;
  late List<PusherClient> clients;
  late List<String> events;

  PusherClient makeClient({
    int activityTimeout = 120000,
    int pongTimeout = 30000,
    int? maxReconnectionAttempts,
    Duration minReconnectDelay = const Duration(milliseconds: 10),
    Duration? maxReconnectDelay,
    Duration healthyConnectionAfter = const Duration(seconds: 10),
    Future<Map<String, String>> Function()? authHeaders,
  }) {
    final c = PusherClient(
      options: PusherOptions(
        key: 'app-key',
        host: server.host,
        authOptions: PusherAuthOptions(server.authEndpoint, headersAsync: authHeaders),
        autoConnect: false,
        activityTimeout: activityTimeout,
        pongTimeout: pongTimeout,
        maxReconnectionAttempts: maxReconnectionAttempts,
        minReconnectDelay: minReconnectDelay,
        maxReconnectDelay: maxReconnectDelay ?? minReconnectDelay * 5,
      ),
      healthyConnectionAfter: healthyConnectionAfter,
    );
    c.onDisconnected((state) => events.add('disconnected ${state is Disconnected ? state.code : null}'));
    c.onConnectionError((error) => events.add('connection_error'));
    c.onError((error) => events.add('error $error'));
    clients.add(c);
    return c;
  }

  setUp(() async {
    events = [];
    clients = [];
    server = FakePusherServer();
    await server.start();
  });

  tearDown(() async {
    for (final c in clients) {
      c.disconnect();
    }
    await server.stop();
  });

  group('reconnects', () {
    test('and re-subscribes after the server closes with 4200 (deploy)', () async {
      final client = makeClient()..connect();
      await eventually(() => client.socketId != null);
      client.subscribe('inbox');
      await eventually(() => server.subscribes('inbox') == 1);

      await server.closeLatest(4200, 'shutting down');

      await eventually(() => server.established == 2 && server.subscribes('inbox') == 2);
      expect(client.connected, isTrue);
      expect(client.connectionState, isA<Reconnected>());
      expect(events, contains('disconnected 4200'));
      expect(server.upgradeRequests, 2);
    });

    for (final code in [1000, 1001, 4201, 4300, null]) {
      test('after close code $code, with exactly one connection open', () async {
        final client = makeClient()..connect();
        await eventually(() => client.socketId != null);

        await server.closeLatest(code, 'bye');

        await eventually(() => server.established == 2 && client.socketId != null);
        await wait(200);
        expect(server.upgradeRequests, 2);
        expect(server.open, 1);
        expect(client.connected, isTrue);
      });
    }

    test('after close code 4100 with a backoff, with exactly one connection open', () async {
      final client = makeClient(minReconnectDelay: const Duration(milliseconds: 200))..connect();
      await eventually(() => client.socketId != null);

      final closedAt = DateTime.now();
      await server.closeLatest(4100, 'over capacity');

      await eventually(() => server.established == 2 && client.socketId != null);
      // 4100-4199 never retry sooner than minReconnectDelay.
      expect(server.upgradeTimes.last.difference(closedAt), greaterThanOrEqualTo(const Duration(milliseconds: 190)));
      await wait(300);
      expect(server.upgradeRequests, 2);
      expect(server.open, 1);
    });

    for (final status in [502, 503]) {
      test('while the server answers HTTP $status, then connects', () async {
        server.rejectWithStatus = status;
        final client = makeClient()..connect();

        await eventually(() => server.upgradeRequests >= 5);
        expect(events, contains('connection_error'));
        server.rejectWithStatus = null;

        await eventually(() => client.socketId != null);
        expect(client.connected, isTrue);
        await wait(200);
        expect(server.open, 1);
      });
    }

    test('through HTTP 503s after an established connection drops', () async {
      final client = makeClient()..connect();
      await eventually(() => client.socketId != null);
      client.subscribe('inbox');
      await eventually(() => server.subscribes('inbox') == 1);

      server.rejectWithStatus = 503;
      await server.closeLatest(4200, 'shutting down');
      await eventually(() => server.upgradeRequests >= 6);
      server.rejectWithStatus = null;

      await eventually(() => server.established == 2 && server.subscribes('inbox') == 2);
    });

    test('after its own activity timeout', () async {
      server.answerPings = false;
      final client = makeClient(activityTimeout: 100, pongTimeout: 100)..connect();
      await eventually(() => client.socketId != null);
      client.subscribe('inbox');
      await eventually(() => server.subscribes('inbox') == 1);

      await eventually(() => server.established >= 2 && server.subscribes('inbox') >= 2);
      server.answerPings = true;
      expect(events, contains('error Activity timeout'));
      expect(events, contains('disconnected null'));
    });

    test('never gives up by default', () async {
      server.rejectWithStatus = 502;
      makeClient(minReconnectDelay: const Duration(milliseconds: 1), maxReconnectDelay: const Duration(milliseconds: 5)).connect();

      await eventually(() => server.upgradeRequests >= 50);
    });
  });

  test('stays connected while the server answers pings', () async {
    final client = makeClient(activityTimeout: 50, pongTimeout: 100)..connect();
    await eventually(() => client.socketId != null);

    await wait(500);
    expect(server.received.where((e) => e['event'] == 'pusher:ping').length, greaterThan(2));
    expect(server.established, 1);
    expect(client.connected, isTrue);
  });

  group('close codes 4000-4099', () {
    for (final code in [4000, 4001, 4099]) {
      test('$code: does not reconnect', () async {
        final client = makeClient()..connect();
        await eventually(() => client.socketId != null);

        await server.closeLatest(code, 'refused');

        await eventually(() => client.connectionState == Disconnected(code: code, reason: 'refused'));
        await wait(500);
        expect(server.upgradeRequests, 1);
        expect(server.open, 0);
        expect(client.connected, isFalse);
      });
    }
  });

  group('backoff', () {
    for (final code in [4200, 4201, 1000, null]) {
      for (final establish in [false, true]) {
        test('a server that accepts then closes ($code${establish ? ', after connection_established' : ''}) is retried with a growing delay', () async {
          server.closeAfterAccept = code;
          server.closeAfterAcceptWithoutCode = code == null;
          server.establishBeforeClosing = establish;
          makeClient(minReconnectDelay: const Duration(milliseconds: 50), maxReconnectDelay: const Duration(seconds: 1)).connect();

          await wait(2000);

          // Delays of 0-100, 50-100, 100-200, 200-400, 400-800, 500-1000 ms...
          // A reconnect loop without delay makes thousands.
          expect(server.upgradeRequests, inInclusiveRange(4, 10));
        });
      }
    }

    test('starts over once a connection stayed up', () async {
      final client = makeClient(
        minReconnectDelay: const Duration(milliseconds: 100),
        maxReconnectDelay: const Duration(seconds: 2),
        healthyConnectionAfter: const Duration(milliseconds: 100),
      )..connect();
      await eventually(() => client.socketId != null);

      for (var i = 2; i <= 6; i++) {
        await wait(150);
        final closedAt = DateTime.now();
        await server.closeLatest(4200);
        await eventually(() => server.established == i);
        // Always a first attempt: 0-200 ms, not a growing delay.
        expect(server.upgradeTimes.last.difference(closedAt), lessThan(const Duration(milliseconds: 400)));
      }
    });

    test('jitters the first attempt after a deploy (4200)', () async {
      final clientCount = 20;
      for (var i = 0; i < clientCount; i++) {
        makeClient(minReconnectDelay: const Duration(milliseconds: 250)).connect();
      }
      await eventually(() => server.established == clientCount);

      final closedAt = DateTime.now();
      for (final socket in List.of(server.sockets)) {
        await socket.close(4200, 'shutting down');
      }
      await eventually(() => server.established == clientCount * 2);

      final delays = server.upgradeTimes.skip(clientCount).map((t) => t.difference(closedAt).inMilliseconds).toList()..sort();
      // Spread over 0-500 ms rather than all at once.
      expect(delays.last - delays.first, greaterThan(150));
      expect(delays.where((d) => d > 100), isNotEmpty);
      expect(delays.last, lessThan(800));
    });
  });

  group('disconnect() and connect()', () {
    test('disconnect() stops reconnecting', () async {
      server.rejectWithStatus = 502;
      final client = makeClient()..connect();
      await eventually(() => server.upgradeRequests >= 3);

      client.disconnect();
      await wait(100);
      final requests = server.upgradeRequests;
      await wait(500);

      expect(server.upgradeRequests, requests);
      expect(client.connectionState, isA<Disconnected>());
    });

    test('disconnect() cancels a scheduled reconnect', () async {
      server.answerPings = false;
      final client = makeClient(activityTimeout: 50, pongTimeout: 50, minReconnectDelay: const Duration(seconds: 1))..connect();
      await eventually(() => client.connectionState is Reconnecting);

      client.disconnect();
      final requests = server.upgradeRequests;
      await wait(2500);

      expect(server.upgradeRequests, requests);
      expect(server.open, 0);
      expect(client.connectionState, isA<Disconnected>());
    });

    test('disconnect() while a connection attempt is in flight leaves nothing open', () async {
      final client = makeClient()..connect();
      await eventually(() => client.socketId != null);
      server.acceptDelay = const Duration(milliseconds: 300);

      await server.closeLatest(4200, 'deploy');
      await eventually(() => server.upgradeRequests == 2);
      client.disconnect();

      await wait(1000);
      expect(server.upgradeRequests, 2);
      expect(server.open, 0);
      expect(client.connectionState, isA<Disconnected>());
    });

    test('connect() while a connection attempt is in flight leaves one connection open', () async {
      final client = makeClient()..connect();
      await eventually(() => client.socketId != null);
      server.acceptDelay = const Duration(milliseconds: 300);

      await server.closeLatest(4200, 'deploy');
      await eventually(() => server.upgradeRequests == 2);
      client.connect();

      await eventually(() => client.socketId != null);
      await wait(1000);
      expect(server.upgradeRequests, 3);
      expect(server.open, 1);
      expect(client.connected, isTrue);
    });

    test('without a connection do not throw', () {
      final client = makeClient();
      client.disconnect();
      client.sendEvent('client-event', {'a': 1});
      client.unsubscribe('inbox');
      expect(client.connectionState, isA<Disconnected>());
    });
  });

  test('gives up after maxReconnectionAttempts when set', () async {
    server.rejectWithStatus = 502;
    final client = makeClient(maxReconnectionAttempts: 2)..connect();

    await eventually(() => client.connectionState == const Disconnected(reason: 'Max reconnection attempts reached'));
    await wait(200);
    expect(server.upgradeRequests, 3);
  });

  group('private channels', () {
    const name = 'private-api.platform.v1.organizations.org.messages';

    test('authorize and subscribe once per connection, even when subscribed again meanwhile', () async {
      final client = makeClient();
      // The app also subscribes when the connection is established.
      client.onConnectionEstablished((_) => client.private(name, subscribe: true));
      client.connect();
      await eventually(() => client.socketId != null);
      client.private(name, subscribe: true);
      await eventually(() => client.channel(name).subscribed);

      await server.closeLatest(4200, 'deploy');
      await eventually(() => server.established == 2 && client.channel(name).subscribed);
      await wait(300);

      expect(server.authRequests, 2);
      expect(server.subscribes(name), 2);
    });

    for (final status in [429, 500, 503]) {
      test('retry a failed auth (HTTP $status) while connected', () async {
        final client = makeClient(minReconnectDelay: const Duration(milliseconds: 20));
        final errors = <dynamic>[];
        client.connect();
        await eventually(() => client.socketId != null);
        final channel = client.private(name)..onSubscriptionError(errors.add);
        server.authFailureStatus = status;
        server.authFailuresLeft = 2;
        channel.subscribe();

        await eventually(() => channel.subscribed);
        expect(server.authRequests, 3);
        expect(errors, hasLength(2));
        expect(server.established, 1);
      });
    }

    test('retry after a failed auth right after a reconnect', () async {
      final client = makeClient(minReconnectDelay: const Duration(milliseconds: 20))..connect();
      await eventually(() => client.socketId != null);
      client.private(name, subscribe: true);
      await eventually(() => client.channel(name).subscribed);

      server.authFailuresLeft = 1;
      await server.closeLatest(4200, 'deploy');

      await eventually(() => server.established == 2 && client.channel(name).subscribed);
      expect(server.authRequests, 3);
    });

    test('retry when the auth headers throw (e.g. no ID token)', () async {
      var calls = 0;
      final client = makeClient(
        minReconnectDelay: const Duration(milliseconds: 20),
        authHeaders: () async {
          if (++calls == 1) throw Exception('token refresh failed');
          return {'Accept': 'application/json'};
        },
      )..connect();
      await eventually(() => client.socketId != null);

      final errors = <dynamic>[];
      final channel = client.private(name)..onSubscriptionError(errors.add);
      channel.subscribe();

      await eventually(() => channel.subscribed);
      expect(calls, 2);
      expect(errors, hasLength(1));
    });

    test('retry when the server answers pusher:subscription_error', () async {
      final client = makeClient(minReconnectDelay: const Duration(milliseconds: 20))..connect();
      await eventually(() => client.socketId != null);
      server.subscriptionErrorsLeft = 1;

      final channel = client.private(name, subscribe: true);

      await eventually(() => channel.subscribed);
      expect(server.subscribes(name), 2);
      expect(server.authRequests, 2);
    });

    test('stop retrying once unsubscribed', () async {
      final client = makeClient(minReconnectDelay: const Duration(milliseconds: 100))..connect();
      await eventually(() => client.socketId != null);
      server.authFailuresLeft = 100;

      final channel = client.private(name, subscribe: true);
      await eventually(() => server.authRequests == 1);
      await wait(20);
      channel.unsubscribe();

      await wait(800);
      expect(server.authRequests, 1);
    });
  });
}
