import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

class MeshGattConstants {
  static const String serviceUuid = "6e400001-b5a3-f393-e0a9-e50e24dcca9e";
  static const String rxWriteCharacteristicUuid =
      "6e400002-b5a3-f393-e0a9-e50e24dcca9e";
  static const String txNotifyCharacteristicUuid =
      "6e400003-b5a3-f393-e0a9-e50e24dcca9e";

  static const int targetMtu = 512;
  static const int defaultMtu = 23;
  static const int attHeaderOverhead = 3;
  static const int defaultTtl = 3;
  static const String eomDelimiter = "<EOM>";
}

class MeshMessage {
  final String id;
  final String sender;
  final String? targetPeer; // null or 'ALL' for broadcast, username/id for 1-on-1
  final String text;
  final int ttl;
  final int timestamp;
  final bool isLocal;
  final bool isDelivered;

  const MeshMessage({
    required this.id,
    required this.sender,
    this.targetPeer,
    required this.text,
    required this.ttl,
    required this.timestamp,
    this.isLocal = false,
    this.isDelivered = true,
  });

  int get byteSize => utf8.encode(toWireJsonString()).length;

  bool get isBroadcast => targetPeer == null || targetPeer == 'ALL';

  MeshMessage copyWith({
    int? ttl,
    bool? isLocal,
    bool? isDelivered,
  }) {
    return MeshMessage(
      id: id,
      sender: sender,
      targetPeer: targetPeer,
      text: text,
      ttl: ttl ?? this.ttl,
      timestamp: timestamp,
      isLocal: isLocal ?? this.isLocal,
      isDelivered: isDelivered ?? this.isDelivered,
    );
  }

  /// Strict wire format matching protocol specification
  Map<String, dynamic> toWireJson() {
    final map = <String, dynamic>{
      'id': id,
      'sender': sender,
      'text': text,
      'ttl': ttl,
      'timestamp': timestamp,
    };
    if (targetPeer != null && targetPeer != 'ALL') {
      map['target'] = targetPeer;
    }
    return map;
  }

  String toWireJsonString() => jsonEncode(toWireJson());

  factory MeshMessage.fromWireJson(Map<String, dynamic> json, {bool isLocal = false}) {
    return MeshMessage(
      id: json['id'] as String,
      sender: json['sender'] as String,
      targetPeer: json['target'] as String?,
      text: json['text'] as String,
      ttl: (json['ttl'] as num).toInt(),
      timestamp: (json['timestamp'] as num).toInt(),
      isLocal: isLocal,
      isDelivered: true,
    );
  }

  Map<String, dynamic> toStorageMap() {
    return {
      'id': id,
      'sender': sender,
      'targetPeer': targetPeer,
      'text': text,
      'ttl': ttl,
      'timestamp': timestamp,
      'isLocal': isLocal,
      'isDelivered': isDelivered,
    };
  }

  factory MeshMessage.fromStorageMap(Map<dynamic, dynamic> map) {
    return MeshMessage(
      id: map['id'] as String,
      sender: map['sender'] as String,
      targetPeer: map['targetPeer'] as String?,
      text: map['text'] as String,
      ttl: (map['ttl'] as num).toInt(),
      timestamp: (map['timestamp'] as num).toInt(),
      isLocal: map['isLocal'] as bool? ?? false,
      isDelivered: map['isDelivered'] as bool? ?? true,
    );
  }
}

class PeerNode {
  final String deviceId;
  String username;
  int rssi;
  bool isConnected;
  int negotiatedMtu;
  DateTime lastSeen;

  PeerNode({
    required this.deviceId,
    required this.username,
    required this.rssi,
    this.isConnected = false,
    this.negotiatedMtu = MeshGattConstants.defaultMtu,
    DateTime? lastSeen,
  }) : lastSeen = lastSeen ?? DateTime.now();

  /// Estimates distance in meters using the Log-Distance Path Loss model:
  /// d = 10 ^ ((TxPower - RSSI) / (10 * n))
  /// where TxPower = -59 dBm at 1m and path loss exponent n = 2.2
  double get estimatedDistanceMeters {
    if (rssi == 0) return -1.0;
    const int txPower = -59;
    const double pathLossExponent = 2.2;
    final double ratio = (txPower - rssi) / (10 * pathLossExponent);
    final double distance = pow(10, ratio).toDouble();
    return double.parse(distance.clamp(0.1, 99.9).toStringAsFixed(1));
  }

  /// Deterministic radar angle (0 to 2*pi) derived from deviceId hash
  double get radarAngleRadians {
    final int hash = deviceId.codeUnits.fold(0, (prev, elem) => (prev * 31 + elem) & 0x7FFFFFFF);
    return (hash % 360) * (pi / 180.0);
  }
}

/// Handles splitting UTF-8 payloads into numbered MTU-safe chunks with an `<EOM>`
/// delimiter, and reassembling incoming byte streams per remote device.
class MeshChunkAssembler {
  final Map<String, BytesBuilder> _incomingBuffers = {};
  final Map<String, DateTime> _bufferTimestamps = {};

  /// Splits a MeshMessage into MTU-sized byte chunks.
  /// Frame format: `<chunk_index>/<total_chunks>|<utf8_slice>` with `<EOM>` appended to final chunk.
  static List<Uint8List> chunkMessage(MeshMessage message, int negotiatedMtu) {
    final String rawJson = message.toWireJsonString();
    final String framedPayload = "$rawJson${MeshGattConstants.eomDelimiter}";
    final Uint8List fullBytes = Uint8List.fromList(utf8.encode(framedPayload));

    final int maxPayloadBytes =
        max(16, negotiatedMtu - MeshGattConstants.attHeaderOverhead - 8);

    if (fullBytes.length <= maxPayloadBytes) {
      final header = utf8.encode("0/1|");
      return [Uint8List.fromList([...header, ...fullBytes])];
    }

    final int totalChunks = (fullBytes.length / maxPayloadBytes).ceil();
    final List<Uint8List> chunks = [];

    for (int i = 0; i < totalChunks; i++) {
      final int start = i * maxPayloadBytes;
      final int end = min(start + maxPayloadBytes, fullBytes.length);
      final Uint8List slice = fullBytes.sublist(start, end);
      final List<int> header = utf8.encode("$i/$totalChunks|");
      chunks.add(Uint8List.fromList([...header, ...slice]));
    }

    return chunks;
  }

  /// Processes an incoming chunk from [sourceDeviceId].
  /// Returns a decoded [MeshMessage] once the `<EOM>` marker is reached, or null if more chunks are awaited.
  MeshMessage? processIncomingBytes(String sourceDeviceId, List<int> rawBytes) {
    _cleanupStaleBuffers();

    String decodedChunk;
    try {
      decodedChunk = utf8.decode(rawBytes, allowMalformed: true);
    } catch (_) {
      return null;
    }

    // Strip chunk header "i/n|" if present
    final int pipeIndex = decodedChunk.indexOf('|');
    List<int> payloadBytes = rawBytes;
    if (pipeIndex != -1 && pipeIndex < 10) {
      final String prefix = decodedChunk.substring(0, pipeIndex);
      if (prefix.contains('/')) {
        final int byteOffset = utf8.encode("$prefix|").length;
        if (byteOffset < rawBytes.length) {
          payloadBytes = rawBytes.sublist(byteOffset);
        }
      }
    }

    final BytesBuilder builder =
        _incomingBuffers.putIfAbsent(sourceDeviceId, () => BytesBuilder());
    builder.add(payloadBytes);
    _bufferTimestamps[sourceDeviceId] = DateTime.now();

    final String accumulated =
        utf8.decode(builder.toBytes(), allowMalformed: true);

    if (accumulated.contains(MeshGattConstants.eomDelimiter)) {
      final int eomIndex = accumulated.indexOf(MeshGattConstants.eomDelimiter);
      final String completeJsonStr = accumulated.substring(0, eomIndex).trim();

      // Clear buffer or retain overflow bytes if back-to-back packets arrived
      final String remaining = accumulated
          .substring(eomIndex + MeshGattConstants.eomDelimiter.length);
      _incomingBuffers.remove(sourceDeviceId);
      _bufferTimestamps.remove(sourceDeviceId);

      if (remaining.isNotEmpty) {
        final BytesBuilder nextBuilder = BytesBuilder();
        nextBuilder.add(utf8.encode(remaining));
        _incomingBuffers[sourceDeviceId] = nextBuilder;
        _bufferTimestamps[sourceDeviceId] = DateTime.now();
      }

      try {
        final Map<String, dynamic> decodedJson =
            jsonDecode(completeJsonStr) as Map<String, dynamic>;
        return MeshMessage.fromWireJson(decodedJson, isLocal: false);
      } catch (_) {
        return null;
      }
    }

    return null;
  }

  void _cleanupStaleBuffers() {
    final DateTime now = DateTime.now();
    final List<String> expiredKeys = _bufferTimestamps.entries
        .where((entry) => now.difference(entry.value).inSeconds > 30)
        .map((entry) => entry.key)
        .toList();
    for (final key in expiredKeys) {
      _incomingBuffers.remove(key);
      _bufferTimestamps.remove(key);
    }
  }
}
