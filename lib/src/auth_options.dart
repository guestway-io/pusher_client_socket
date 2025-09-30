import 'package:flutter/foundation.dart';

/// Represents the options for the authentication.
class PusherAuthOptions {
  /// The endpoint for the authentication.
  final String endpoint;

  /// The headers for the authentication (default: `{'Accept': 'application/json'}`).
  /// If you need to provide headers asynchronously, use [headersAsync].
  final Map<String, String> headers;

  /// Async headers for the authentication, will override [headers] if provided.
  final AsyncValueGetter<Map<String, String>>? headersAsync;

  const PusherAuthOptions(this.endpoint, {this.headers = const {'Accept': 'application/json'}, this.headersAsync});

  /// Returns a new [PusherAuthOptions] with the given endpoint.
  @override
  String toString() {
    return 'AuthOptions(endpoint: $endpoint, headers: $headers, headersAsync: ${headersAsync != null})';
  }
}
