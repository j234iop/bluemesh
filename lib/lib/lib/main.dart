import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:ble_peripheral/ble_peripheral.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart';
import 'package:hive_flutter/hive_flutter.dart';
import 'package:uuid/uuid.dart';

import 'mesh_protocol.dart';

class BleMeshManager extends ChangeNotifier {
  static final BleMeshManager _instance = BleMeshManager._internal();
  factory BleMeshManager() => _instance;
  BleMeshManager._internal();

  final Uuid _uuid = const Uuid();
  final MeshChunkAssembler _chunkAssembler = MeshChunkAssembler();

  /// Deduplication set for flood-mesh relaying
  final Set<String> _seenMessageIds = <String>{};

  /// Discovered and connected peers keyed by deviceId
  final Map<String, PeerNode> _peers = <String, PeerNode>{};
  final Map<String, BluetoothDevice> _connectedCentralDevices = {};
  final Map<String, BluetoothCharacteristic> _peerRxCharacteristics = {};
  final Map<String, StreamSubscription<List<int>>> _notifySubscriptions = {};

  /// Subscribers connected to our local GATT server
  final Set<String> _connectedPeripheralSubscribers = <String>{};

  late Box _messagesBox;
  late Box _settingsBox;

  String _localUsername = "Node-${DateTime.now().millisecondsSinceEpoch % 1000}";
  bool _isScanning = false;
  bool _isAdvertising = false;
  bool _isInitialized = false;
  String _statusBanner = "Idle";

  StreamSubscription<List<ScanResult>>? _scanSubscription;
  Timer? _autoScanTimer;

  // Public Getters
  String get localUsername => _localUsername;
  bool get isScanning => _isScanning;
  bool get isAdvertising => _isAdvertising;
  bool get isInitialized => _isInitialized;
  String get statusBanner => _statusBanner;

  List<PeerNode> get peersList {
    final list = _peers.values.toList();
    list.sort((a, b) {
      if (a.isConnected != b.isConnected) {
        return a.isConnected ? -1 : 1;
      }
      return b.rssi.compareTo(a.rssi);
    });
    return list;
  }

  int get connectedPeersCount =>
      _peers.values.where((p) => p.isConnected).length +
      _connectedPeripheralSubscribers.length;

  List<MeshMessage> get allMessages {
    if (!_isInitialized) return [];
    final List<MeshMessage> msgs = _messagesBox.values
        .map((e) => MeshMessage.fromStorageMap(e as Map<dynamic, dynamic>))
        .toList();
    msgs.sort((a, b) => a.timestamp.compareTo(b.timestamp));
    return msgs;
  }

  List<MeshMessage> get broadcastMessages =>
      allMessages.where((m) => m.isBroadcast).toList();

  List<MeshMessage> messagesForPeer(String peerUsername) {
    return allMessages.where((m) {
      if (m.isBroadcast) return false;
      final bool sentByMeToPeer =
          m.sender == _localUsername && m.targetPeer == peerUsername;
      final bool sentByPeerToMe =
          m.sender == peerUsername &&
          (m.targetPeer == _localUsername || m.targetPeer == peerUsername);
      return sentByMeToPeer || sentByPeerToMe;
    }).toList();
  }

  Future<void> initStorage() async {
    await Hive.initFlutter();
    _messagesBox = await Hive.openBox('bluemesh_messages');
    _settingsBox = await Hive.openBox('bluemesh_settings');

    final String? savedName = _settingsBox.get('username') as String?;
    if (savedName != null && savedName.trim().isNotEmpty) {
      _localUsername = savedName.trim();
    }

    // Pre-populate seen message IDs from local storage so we don't re-relay old messages
    for (final raw in _messagesBox.values) {
      final msg = MeshMessage.fromStorageMap(raw as Map<dynamic, dynamic>);
      _seenMessageIds.add(msg.id);
    }

    _isInitialized = true;
    notifyListeners();
  }

  bool get hasConfiguredProfile =>
      _settingsBox.isOpen &&
      (_settingsBox.get('username') as String?)?.isNotEmpty == true;

  Future<void> setUsername(String name) async {
    _localUsername = name.trim();
    await _settingsBox.put('username', _localUsername);
    notifyListeners();
  }

  /// Starts both GATT Peripheral Server + Advertising and GATT Central Scanning
  Future<void> startMeshEngine() async {
    _updateStatus("Starting Dual-Role BLE Mesh Engine...");
    await _initPeripheralGattServer();
    await startScanning();

    _autoScanTimer?.cancel();
    _autoScanTimer = Timer.periodic(const Duration(seconds: 25), (_) {
      if (!_isScanning) {
        startScanning();
      }
    });
  }

  Future<void> stopMeshEngine() async {
    _autoScanTimer?.cancel();
    await stopScanning();
    try {
      await BlePeripheral.stopAdvertising();
      await BlePeripheral.clearServices();
    } catch (_) {}
    _isAdvertising = false;
    _updateStatus("Mesh Engine Stopped");
  }

  // ===========================================================================
  // 1. PERIPHERAL ROLE: GATT SERVER & ADVERTISING (ble_peripheral)
  // ===========================================================================
  Future<void> _initPeripheralGattServer() async {
    try {
      await BlePeripheral.initialize();

      BlePeripheral.setAdvertisingStatusUpdateCallback(
        (bool advertising, String? error) {
          _isAdvertising = advertising;
          if (error != null) {
            _updateStatus("Advertising error: $error");
          } else {
            _updateStatus(advertising
                ? "Active: Advertising as $_localUsername"
                : "Advertising paused");
          }
          notifyListeners();
        },
      );

      BlePeripheral.setCharacteristicSubscriptionChangeCallback((
        String deviceId,
        String characteristicId,
        bool isSubscribed,
        String? name,
      ) {
        if (isSubscribed) {
          _connectedPeripheralSubscribers.add(deviceId);
          _upsertPeer(
            deviceId: deviceId,
            username: name ?? "Peer-${deviceId.substring(0, 4)}",
            rssi: -55,
            isConnected: true,
          );
        } else {
          _connectedPeripheralSubscribers.remove(deviceId);
        }
        notifyListeners();
      });

      // Handle incoming writes on RX Characteristic from remote Centrals
      BlePeripheral.setWriteRequestCallback((
        String deviceId,
        String characteristicId,
        int offset,
        Uint8List? value,
      ) {
        if (value != null && value.isNotEmpty) {
          _handleIncomingRawBytes(deviceId, value);
        }
        return WriteRequestResult(status: 0);
      });

      BlePeripheral.setReadRequestCallback((
        String deviceId,
        String characteristicId,
        int offset,
        Uint8List? value,
      ) {
        final Uint8List nameBytes =
            Uint8List.fromList(_localUsername.codeUnits);
        return ReadRequestResult(value: nameBytes);
      });

      await BlePeripheral.clearServices();

      await BlePeripheral.addService(
        BleService(
          uuid: MeshGattConstants.serviceUuid,
          primary: true,
          characteristics: [
            BleCharacteristic(
              uuid: MeshGattConstants.rxWriteCharacteristicUuid,
              properties: [
                CharacteristicProperties.write.index,
                CharacteristicProperties.writeWithoutResponse.index,
              ],
              permissions: [
                AttributePermissions.writeable.index,
              ],
            ),
            BleCharacteristic(
              uuid: MeshGattConstants.txNotifyCharacteristicUuid,
              properties: [
                CharacteristicProperties.read.index,
                CharacteristicProperties.notify.index,
              ],
              permissions: [
                AttributePermissions.readable.index,
              ],
              descriptors: [
                BleDescriptor(
                  uuid: "00002902-0000-1000-8000-00805f9b34fb",
                  permissions: [
                    AttributePermissions.readable.index,
                    AttributePermissions.writeable.index,
                  ],
                ),
              ],
            ),
          ],
        ),
      );

      await BlePeripheral.startAdvertising(
        services: [MeshGattConstants.serviceUuid],
        localName: _localUsername,
      );

      _isAdvertising = true;
      notifyListeners();
    } catch (e) {
      _updateStatus("Peripheral init warning: $e");
    }
  }

  // ===========================================================================
  // 2. CENTRAL ROLE: SCANNING, MTU NEGOTIATION & GATT CLIENT (flutter_blue_plus)
  // ===========================================================================
  Future<void> startScanning() async {
    if (_isScanning) return;

    try {
      _isScanning = true;
      notifyListeners();

      _scanSubscription?.cancel();
      _scanSubscription = FlutterBluePlus.onScanResults.listen((results) {
        for (final ScanResult result in results) {
          final String deviceId = result.device.remoteId.str;
          final String advName = result.advertisementData.advName.isNotEmpty
              ? result.advertisementData.advName
              : (result.device.platformName.isNotEmpty
                  ? result.device.platformName
                  : "MeshNode-${deviceId.substring(deviceId.length > 4 ? deviceId.length - 4 : 0)}");

          _upsertPeer(
            deviceId: deviceId,
            username: advName,
            rssi: result.rssi,
          );

          // Automatically connect to discovered mesh nodes for zero-touch multi-hop relay
          if (!_connectedCentralDevices.containsKey(deviceId)) {
            connectToPeer(result.device, discoveredName: advName);
          }
        }
      });

      await FlutterBluePlus.startScan(
        withServices: [Guid(MeshGattConstants.serviceUuid)],
        timeout: const Duration(seconds: 15),
        androidUsesFineLocation: true,
      );

      Future.delayed(const Duration(seconds: 15), () {
        _isScanning = false;
        notifyListeners();
      });
    } catch (e) {
      _isScanning = false;
      _updateStatus("Scan error: $e");
      notifyListeners();
    }
  }

  Future<void> stopScanning() async {
    await FlutterBluePlus.stopScan();
    await _scanSubscription?.cancel();
    _isScanning = false;
    notifyListeners();
  }

  Future<void> connectToPeer(
    BluetoothDevice device, {
    String? discoveredName,
  }) async {
    final String deviceId = device.remoteId.str;
    if (_connectedCentralDevices.containsKey(deviceId)) return;

    try {
      await device.connect(
        timeout: const Duration(seconds: 10),
        autoConnect: false,
      );
      _connectedCentralDevices[deviceId] = device;

      device.connectionState.listen((BluetoothConnectionState state) {
        if (state == BluetoothConnectionState.disconnected) {
          _connectedCentralDevices.remove(deviceId);
          _peerRxCharacteristics.remove(deviceId);
          _notifySubscriptions.remove(deviceId)?.cancel();
          if (_peers.containsKey(deviceId)) {
            _peers[deviceId]!.isConnected = false;
            notifyListeners();
          }
        }
      });

      // Negotiate 512-byte MTU on Android (iOS negotiates MTU automatically)
      int negotiatedMtu = 185;
      if (Platform.isAndroid) {
        try {
          negotiatedMtu =
              await device.requestMtu(MeshGattConstants.targetMtu);
        } catch (_) {
          negotiatedMtu = MeshGattConstants.defaultMtu;
        }
      }

      // Discover GATT Services & match RX/TX Characteristics
      final List<BluetoothService> services = await device.discoverServices();
      for (final BluetoothService service in services) {
        if (service.uuid.str.toLowerCase() ==
            MeshGattConstants.serviceUuid.toLowerCase()) {
          for (final BluetoothCharacteristic c in service.characteristics) {
            final String charUuid = c.uuid.str.toLowerCase();

            if (charUuid ==
                MeshGattConstants.rxWriteCharacteristicUuid.toLowerCase()) {
              _peerRxCharacteristics[deviceId] = c;
            } else if (charUuid ==
                MeshGattConstants.txNotifyCharacteristicUuid.toLowerCase()) {
              // Read peer handle if available
              if (c.properties.read) {
                try {
                  final List<int> nameBytes = await c.read();
                  if (nameBytes.isNotEmpty) {
                    discoveredName = String.fromCharCodes(nameBytes);
                  }
                } catch (_) {}
              }
              // Subscribe to TX notifications from peer
              if (c.properties.notify) {
                try {
                  await c.setNotifyValue(true);
                  _notifySubscriptions[deviceId]?.cancel();
                  _notifySubscriptions[deviceId] =
                      c.onValueReceived.listen((List<int> bytes) {
                    _handleIncomingRawBytes(deviceId, bytes);
                  });
                } catch (_) {}
              }
            }
          }
        }
      }

      _upsertPeer(
        deviceId: deviceId,
        username: discoveredName ?? _peers[deviceId]?.username ?? "MeshPeer",
        rssi: _peers[deviceId]?.rssi ?? -60,
        isConnected: true,
        negotiatedMtu: negotiatedMtu,
      );
      _updateStatus("Connected to ${_peers[deviceId]?.username} (MTU: $negotiatedMtu B)");
    } catch (e) {
      _connectedCentralDevices.remove(deviceId);
    }
  }

  // ===========================================================================
  // 3. PACKET REASSEMBLY & MULTI-HOP FLOOD-MESH RELAYING
  // ===========================================================================
  void _handleIncomingRawBytes(String sourceDeviceId, List<int> rawBytes) {
    final MeshMessage? message =
        _chunkAssembler.processIncomingBytes(sourceDeviceId, rawBytes);
    if (message == null) return;

    // Update peer username if discovered inside message header
    if (_peers.containsKey(sourceDeviceId) &&
        _peers[sourceDeviceId]!.username.startsWith("MeshNode")) {
      _peers[sourceDeviceId]!.username = message.sender;
    } else if (!_peers.containsKey(sourceDeviceId)) {
      _upsertPeer(
        deviceId: sourceDeviceId,
        username: message.sender,
        rssi: -62,
        isConnected: true,
      );
    }

    // Flood-Mesh Deduplication Check
    if (_seenMessageIds.contains(message.id)) {
      return;
    }

    // Mark message ID as seen
    _seenMessageIds.add(message.id);

    // Save to local Hive store and update UI
    _messagesBox.put(message.id, message.toStorageMap());
    notifyListeners();

    // Multi-Hop Relaying: Decrement TTL and re-broadcast if TTL > 0
    final int nextTtl = message.ttl - 1;
    if (nextTtl > 0) {
      final MeshMessage relayedMessage = message.copyWith(
        ttl: nextTtl,
        isLocal: false,
      );
      _transmitOverBle(relayedMessage, excludeDeviceId: sourceDeviceId);
    }
  }

  /// Sends a newly composed message from the local user to the mesh
  Future<void> sendMessage({
    required String text,
    String? targetPeerUsername,
  }) async {
    if (text.trim().isEmpty) return;

    final MeshMessage message = MeshMessage(
      id: _uuid.v4(),
      sender: _localUsername,
      targetPeer: targetPeerUsername ?? 'ALL',
      text: text.trim(),
      ttl: MeshGattConstants.defaultTtl,
      timestamp: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      isLocal: true,
      isDelivered: true,
    );

    _seenMessageIds.add(message.id);
    await _messagesBox.put(message.id, message.toStorageMap());
    notifyListeners();

    await _transmitOverBle(message);
  }

  /// Transmits chunked packets across both Central RX Writes and Peripheral TX Notifications
  Future<void> _transmitOverBle(
    MeshMessage message, {
    String? excludeDeviceId,
  }) async {
    // 1. Write to all connected GATT servers (Central Role)
    for (final entry in _peerRxCharacteristics.entries) {
      final String targetDeviceId = entry.key;
      if (targetDeviceId == excludeDeviceId) continue;

      final BluetoothCharacteristic rxChar = entry.value;
      final int peerMtu =
          _peers[targetDeviceId]?.negotiatedMtu ?? MeshGattConstants.defaultMtu;
      final List<Uint8List> chunks =
          MeshChunkAssembler.chunkMessage(message, peerMtu);

      final bool withoutResponse = rxChar.properties.writeWithoutResponse;

      for (final Uint8List chunk in chunks) {
        try {
          await rxChar.write(chunk, withoutResponse: withoutResponse);
          if (chunks.length > 1) {
            await Future.delayed(const Duration(milliseconds: 18));
          }
        } catch (_) {}
      }
    }

    // 2. Notify all subscribers connected to our local GATT server (Peripheral Role)
    if (_connectedPeripheralSubscribers.isNotEmpty) {
      final List<Uint8List> notifyChunks =
          MeshChunkAssembler.chunkMessage(message, 185);
      for (final String subscriberId in _connectedPeripheralSubscribers) {
        if (subscriberId == excludeDeviceId) continue;
        for (final Uint8List chunk in notifyChunks) {
          try {
            await BlePeripheral.updateCharacteristic(
              characteristicId: MeshGattConstants.txNotifyCharacteristicUuid,
              value: chunk,
              deviceId: subscriberId,
            );
            if (notifyChunks.length > 1) {
              await Future.delayed(const Duration(milliseconds: 18));
            }
          } catch (_) {}
        }
      }
    }
  }

  void _upsertPeer({
    required String deviceId,
    required String username,
    required int rssi,
    bool? isConnected,
    int? negotiatedMtu,
  }) {
    if (_peers.containsKey(deviceId)) {
      final existing = _peers[deviceId]!;
      existing.rssi = rssi;
      existing.lastSeen = DateTime.now();
      if (username.isNotEmpty && !username.startsWith("MeshNode")) {
        existing.username = username;
      }
      if (isConnected != null) existing.isConnected = isConnected;
      if (negotiatedMtu != null) existing.negotiatedMtu = negotiatedMtu;
    } else {
      _peers[deviceId] = PeerNode(
        deviceId: deviceId,
        username: username,
        rssi: rssi,
        isConnected: isConnected ?? false,
        negotiatedMtu: negotiatedMtu ?? MeshGattConstants.defaultMtu,
      );
    }
    notifyListeners();
  }

  Future<void> clearChatHistory() async {
    await _messagesBox.clear();
    _seenMessageIds.clear();
    notifyListeners();
  }

  void _updateStatus(String status) {
    _statusBanner = status;
    notifyListeners();
  }
}
