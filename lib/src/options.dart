import 'dart:convert';
import 'dart:developer' as dev;

import 'package:pinenacl/api.dart';
import 'package:pinenacl/x25519.dart' show SecretBox;

import 'auth_options.dart';

export 'auth_options.dart';

/// Represents the options of the Pusher client.
class PusherOptions {
  /// The key of the connection.
  final String key;

  //// The host of the connection.
  final String? host;

  /// The ws port of the connection (default: 80).
  final int wsPort;

  /// The wss port of the connection (default: 443).
  final int wssPort;

  /// Enable encryption for the connection.
  final bool encrypted;

  /// The cluster of the connection.
  final String? cluster;

  /// The activity timeout of the connection (default: 120000).
  final int activityTimeout;

  /// The pong timeout of the connection (default: 30000).
  final int pongTimeout;

  /// The parameters of the connection.
  final Map<String, String> parameters;

  /// The authentication options of the connection.
  final PusherAuthOptions authOptions;

  /// Whether to enable logging or not.
  final bool enableLogging;

  /// Whether to auto connect or not.
  final bool autoConnect;

  /// How many times in a row the client re-creates its connection without
  /// reaching the server before it gives up (default: `null`, never give up).
  ///
  /// It only counts the connections the client re-creates itself (after its
  /// own activity timeout, a 4100-4199 close code, or once web_socket_client
  /// stopped its own reconnection attempts), and starts over at every
  /// successful `pusher:connection_established`.
  final int? maxReconnectionAttempts;

  /// No longer used: the client reconnects with a capped, jittered exponential
  /// backoff between [minReconnectDelay] and [maxReconnectDelay].
  @Deprecated('Ignored. Use minReconnectDelay and maxReconnectDelay.')
  final Duration reconnectGap;

  /// The base delay before the first reconnection attempt (default: 1 second).
  /// Each failed attempt doubles it, up to [maxReconnectDelay], and every delay
  /// is jittered between half and all of that value.
  final Duration minReconnectDelay;

  /// The longest delay between two reconnection attempts (default: 30 seconds).
  final Duration maxReconnectDelay;

  /// The channel decryption handler.
  final Map<String, dynamic> Function(
    Uint8List sharedSecret,
    Map<String, dynamic> data,
  )? channelDecryption;

  const PusherOptions({
    required this.key,
    this.cluster,
    this.host,
    this.wsPort = 80,
    this.wssPort = 443,
    this.encrypted = true,
    this.activityTimeout = 120000,
    this.pongTimeout = 30000,
    this.parameters = const {
      'client': 'pusher-client-socket-dart',
      'protocol': '7',
      'version': '0.0.2',
      "flash": "false",
    },
    required this.authOptions,
    this.enableLogging = false,
    this.autoConnect = true,
    this.maxReconnectionAttempts,
    @Deprecated('Ignored. Use minReconnectDelay and maxReconnectDelay.') this.reconnectGap = const Duration(seconds: 2),
    this.minReconnectDelay = const Duration(seconds: 1),
    this.maxReconnectDelay = const Duration(seconds: 30),
    this.channelDecryption,
  });

  Uri get uri {
    Uri? hostUri;
    try {
      hostUri = Uri.parse(host!);
      if (hostUri.scheme.isEmpty) {
        hostUri = Uri.parse('${encrypted ? 'wss' : 'ws'}://$host');
      }
    } catch (e) {
      dev.log("Invalid host: $host", error: e);
    }

    return Uri(
      scheme: hostUri?.scheme.isNotEmpty == true
          ? hostUri!.scheme
          : (encrypted ? 'wss' : 'ws'),
      host: hostUri?.host.isNotEmpty == true
          ? hostUri!.host
          : (cluster != null ? 'ws-$cluster.pusher.com' : 'ws.pusher.com'),
      port: hostUri == null || hostUri.port == 0 ? (encrypted ? wssPort : wsPort) : hostUri.port,
      queryParameters: {
        ...parameters,
        if (hostUri?.query.isNotEmpty == true) ...hostUri!.queryParameters,
      },
      path: '/app/$key',
    );
  }

  log(String level, [String? channel, String? message]) {
    if (enableLogging) {
      dev.log([
        "[PUSHER_",
        if (channel != null) "CHANNEL_",
        "$level]",
        if (channel != null) "\n  channel: $channel",
        if (message != null) "\n  $message"
      ].join(""));
    }
  }

  ByteList _decodeCipherText(String cipherText) {
    Uint8List uint8list = base64Decode(cipherText);
    ByteData byteData = ByteData.sublistView(uint8list);
    List<int> data = List<int>.generate(
        byteData.lengthInBytes, (index) => byteData.getUint8(index));
    return ByteList(data);
  }

  Map<String, dynamic> decryptChannelData(
    Uint8List sharedSecret,
    Map<String, dynamic> data,
  ) =>
      (channelDecryption ?? defaultChannelDecryptionHandler)(
        sharedSecret,
        data,
      );

  Map<String, dynamic> defaultChannelDecryptionHandler(
    Uint8List sharedSecret,
    Map<String, dynamic> data,
  ) {
    if (!data.containsKey("ciphertext") || !data.containsKey("nonce")) {
      throw Exception(
        "Unexpected format for encrypted event, expected object with `ciphertext` and `nonce` fields, got: $data",
      );
    }

    final ByteList cipherText = _decodeCipherText(data["ciphertext"]);

    final Uint8List nonce = base64Decode(data["nonce"]);

    final SecretBox secretBox = SecretBox(sharedSecret);
    final Uint8List decryptedData = secretBox.decrypt(cipherText, nonce: nonce);

    return jsonDecode(utf8.decode(decryptedData)) as Map<String, dynamic>;
  }
}
