import 'dart:math';

import 'package:flutter_test/flutter_test.dart';
import 'package:pusher_client_socket/src/reconnect.dart';

void main() {
  group('closeActionFor', () {
    test('does not reconnect after 4000-4099', () {
      for (final code in [4000, 4001, 4004, 4009, 4099]) {
        expect(closeActionFor(code), PusherCloseAction.doNotReconnect, reason: '$code');
      }
    });

    test('backs off after 4100-4199', () {
      for (final code in [4100, 4150, 4199]) {
        expect(closeActionFor(code), PusherCloseAction.reconnectWithBackoff, reason: '$code');
      }
    });

    test('reconnects after 4200-4299 and every other code', () {
      for (final code in [null, 1000, 1001, 1005, 1006, 1011, 3999, 4200, 4201, 4202, 4299, 4300, 4999]) {
        expect(closeActionFor(code), PusherCloseAction.reconnect, reason: '$code');
      }
    });
  });

  group('ReconnectBackoff', () {
    ReconnectBackoff backoff({int seed = 1}) => ReconnectBackoff(
          minDelay: const Duration(seconds: 1),
          maxDelay: const Duration(seconds: 30),
          random: Random(seed),
        );

    test('first attempt: anywhere between 0 and twice the base delay', () {
      final delays = [for (var seed = 0; seed < 200; seed++) backoff(seed: seed).next().inMilliseconds];
      expect(delays, everyElement(inInclusiveRange(0, 2000)));
      expect(delays.where((d) => d < 500), isNotEmpty);
      expect(delays.where((d) => d > 1500), isNotEmpty);
    });

    test('then doubles, jittered between half and all of it, up to the cap', () {
      for (var seed = 0; seed < 50; seed++) {
        final b = backoff(seed: seed)..next();
        final bases = [2000, 4000, 8000, 16000, 30000, 30000, 30000, 30000];
        for (final base in bases) {
          final delay = b.next().inMilliseconds;
          expect(delay, inInclusiveRange(base ~/ 2, base), reason: 'seed $seed, base $base');
        }
      }
    });

    test('minAttempt skips the first steps', () {
      for (var seed = 0; seed < 50; seed++) {
        expect(backoff(seed: seed).next(minAttempt: 1).inMilliseconds, inInclusiveRange(1000, 2000));
      }
    });

    test('never exceeds the cap and never gives up', () {
      final b = backoff();
      for (var i = 0; i < 1000; i++) {
        expect(b.next(), lessThanOrEqualTo(const Duration(seconds: 30)));
      }
      expect(b.attempt, 1000);
    });

    test('jitters: clients that dropped together do not retry together', () {
      final delays = {for (var seed = 0; seed < 20; seed++) backoff(seed: seed).next()};
      expect(delays.length, greaterThan(15));
    });

    test('reset starts over', () {
      final b = backoff();
      for (var i = 0; i < 6; i++) {
        b.next();
      }
      expect(b.next().inMilliseconds, inInclusiveRange(15000, 30000));

      b.reset();
      expect(b.attempt, 0);
      expect(b.next().inMilliseconds, inInclusiveRange(0, 2000));
    });
  });
}
