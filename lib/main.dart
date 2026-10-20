import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:hive_flutter/hive_flutter.dart';
import 'package:http/http.dart' as http;
import 'package:open_filex/open_filex.dart';
import 'package:path_provider/path_provider.dart';
import 'package:receive_sharing_intent/receive_sharing_intent.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

const String serverHost = '192.168.31.6';
const int serverPort = 8080;
const String baseUrl = 'http://$serverHost:$serverPort';
const String wsUrl = 'ws://$serverHost:$serverPort/ws';

const List<String> defaultSpaces = [
  'All',
  'Unfiled',
  'DBMS',
  'Networks',
  'OS',
  'Code',
  'Research',
];

const Color kBgDeep = Color(0xFF050410);
const Color kBgCard = Color(0xFF1E1B2E);
const Color kBgSurface = Color(0xFF161325);
const Color kAccent = Color(0xFF8B5CF6);
const Color kAccentLight = Color(0xFFA78BFA);
const Color kAccentDim = Color(0xFF6D28D9);

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await Hive.initFlutter();
  await Hive.openBox('offline_queue');
  await Hive.openBox('cached_blocks');
  runApp(const StreamSpaceApp());
}

class StreamSpaceApp extends StatelessWidget {
  const StreamSpaceApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'StreamSpace',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        brightness: Brightness.dark,
        scaffoldBackgroundColor: kBgDeep,
        primaryColor: kAccent,
        colorScheme: const ColorScheme.dark(
          primary: kAccent,
          secondary: kAccentLight,
          surface: kBgSurface,
          onPrimary: Colors.white,
          onSecondary: Colors.white,
          onSurface: Colors.white,
        ),
        fontFamily: 'Roboto',
        appBarTheme: const AppBarTheme(
          backgroundColor: kBgSurface,
          elevation: 0,
          surfaceTintColor: Colors.transparent,
          titleTextStyle: TextStyle(
            color: Colors.white,
            fontSize: 20,
            fontWeight: FontWeight.w700,
          ),
        ),
        inputDecorationTheme: InputDecorationTheme(
          filled: true,
          fillColor: kBgCard.withValues(alpha: 0.5),
          border: OutlineInputBorder(
            borderRadius: BorderRadius.circular(14),
            borderSide: BorderSide(color: Colors.white.withValues(alpha: 0.05)),
          ),
          enabledBorder: OutlineInputBorder(
            borderRadius: BorderRadius.circular(14),
            borderSide: BorderSide(color: Colors.white.withValues(alpha: 0.05)),
          ),
          focusedBorder: OutlineInputBorder(
            borderRadius: BorderRadius.circular(14),
            borderSide: const BorderSide(color: kAccent, width: 1.5),
          ),
          contentPadding:
              const EdgeInsets.symmetric(horizontal: 16, vertical: 14),
          hintStyle: TextStyle(color: Colors.grey.shade600, fontSize: 14),
        ),
        chipTheme: ChipThemeData(
          backgroundColor: kBgCard,
          selectedColor: kAccent,
          disabledColor: kBgCard,
          labelStyle: const TextStyle(fontSize: 12, fontWeight: FontWeight.w500),
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(10),
            side: BorderSide(color: Colors.white.withValues(alpha: 0.06)),
          ),
        ),
      ),
      home: const HomeScreen(),
    );
  }
}

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> with WidgetsBindingObserver {
  List<Map<String, dynamic>> _blocks = [];
  String _currentSpace = 'All';
  String _searchQuery = '';
  bool _isLoading = false;
  bool _wsConnected = false;

  final TextEditingController _searchController = TextEditingController();
  final TextEditingController _noteController = TextEditingController();
  final ScrollController _scrollController = ScrollController();

  WebSocketChannel? _wsChannel;
  StreamSubscription? _wsSub;
  Timer? _wsReconnectTimer;
  StreamSubscription? _shareMediaSub;

  late Box _offlineQueue;
  late Box _cachedBlocks;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _offlineQueue = Hive.box('offline_queue');
    _cachedBlocks = Hive.box('cached_blocks');
    _loadCachedBlocks();
    _fetchBlocks();
    _connectWebSocket();
    _flushOfflineQueue();
    _initShareListeners();
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _searchController.dispose();
    _noteController.dispose();
    _scrollController.dispose();
    _wsSub?.cancel();
    _wsChannel?.sink.close();
    _wsReconnectTimer?.cancel();
    _shareMediaSub?.cancel();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed) {
      _fetchBlocks();
      _flushOfflineQueue();
      if (!_wsConnected) _connectWebSocket();
    }
  }

  void _loadCachedBlocks() {
    final cached = _cachedBlocks.get('blocks');
    if (cached != null) {
      try {
        final List<dynamic> decoded = jsonDecode(cached as String);
        setState(() {
          _blocks = decoded.cast<Map<String, dynamic>>();
        });
      } catch (_) {}
    }
  }

  void _cacheBlocks() {
    try {
      _cachedBlocks.put('blocks', jsonEncode(_blocks));
    } catch (_) {}
  }

  void _initShareListeners() {
    _shareMediaSub = ReceiveSharingIntent.instance
        .getMediaStream()
        .listen((List<SharedMediaFile> files) {
      if (files.isNotEmpty) {
        _handleSharedMedia(files);
      }
    }, onError: (err) {
      debugPrint('Share stream error: $err');
    });

    ReceiveSharingIntent.instance
        .getInitialMedia()
        .then((List<SharedMediaFile> files) {
      if (files.isNotEmpty) {
        WidgetsBinding.instance.addPostFrameCallback((_) {
          if (mounted) {
            _handleSharedMedia(files);
            ReceiveSharingIntent.instance.reset();
          }
        });
      }
    });
  }

  void _handleSharedMedia(List<SharedMediaFile> files) {
    _showSpacePickerSheet((space) async {
      for (final file in files) {
        if (file.type == SharedMediaType.text ||
            file.type == SharedMediaType.url) {
          await _sendOrQueue(rawContent: file.path, space: space);
        } else {
          final normalized = file.path.replaceAll(r'\', '/');
          final fileName =
              normalized.contains('/') ? normalized.split('/').last : file.path;
          await _sendOrQueue(
            filePath: file.path,
            space: space,
            rawContent: fileName,
          );
        }
      }
    });
  }

  void _showSpacePickerSheet(Function(String space) onPick) {
    if (!mounted) return;
    showModalBottomSheet(
      context: context,
      backgroundColor: kBgSurface,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(24)),
      ),
      builder: (ctx) {
        return Padding(
          padding: const EdgeInsets.all(24),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Center(
                child: Container(
                  width: 40,
                  height: 4,
                  decoration: BoxDecoration(
                    color: Colors.grey.shade700,
                    borderRadius: BorderRadius.circular(2),
                  ),
                ),
              ),
              const SizedBox(height: 20),
              const Text(
                'Choose a Space',
                style: TextStyle(
                  fontSize: 18,
                  fontWeight: FontWeight.w700,
                  color: Colors.white,
                ),
              ),
              const SizedBox(height: 4),
              Text(
                'Where should this resource be filed?',
                style: TextStyle(fontSize: 13, color: Colors.grey.shade500),
              ),
              const SizedBox(height: 20),
              Wrap(
                spacing: 10,
                runSpacing: 10,
                children: defaultSpaces
                    .where((s) => s != 'All')
                    .map((s) => Material(
                          color: Colors.transparent,
                          child: InkWell(
                            borderRadius: BorderRadius.circular(14),
                            onTap: () {
                              Navigator.pop(ctx);
                              onPick(s);
                            },
                            child: Container(
                              padding: const EdgeInsets.symmetric(
                                  horizontal: 20, vertical: 12),
                              decoration: BoxDecoration(
                                color: kBgCard,
                                borderRadius: BorderRadius.circular(14),
                                border: Border.all(
                                    color: Colors.white.withValues(alpha: 0.06)),
                              ),
                              child: Text(
                                s,
                                style: const TextStyle(
                                  fontSize: 14,
                                  fontWeight: FontWeight.w500,
                                  color: kAccentLight,
                                ),
                              ),
                            ),
                          ),
                        ))
                    .toList(),
              ),
              const SizedBox(height: 16),
            ],
          ),
        );
      },
    );
  }

  Future<void> _sendOrQueue({
    String? filePath,
    String rawContent = '',
    String space = 'Unfiled',
    String annotation = '',
  }) async {
    final payload = <String, String>{
      'raw_content': rawContent,
      'space': space,
      'annotation': annotation,
    };
    if (filePath != null) {
      payload['file_path'] = filePath;
    }

    try {
      final uri = Uri.parse('$baseUrl/api/blocks');
      final request = http.MultipartRequest('POST', uri);
      request.fields['raw_content'] = rawContent;
      request.fields['space'] = space;
      request.fields['annotation'] = annotation;

      if (filePath != null && filePath.isNotEmpty) {
        final file = File(filePath);
        if (await file.exists()) {
          request.files.add(
            await http.MultipartFile.fromPath('file', filePath),
          );
        }
      }

      final streamedResp = await request.send().timeout(
            const Duration(seconds: 60),
          );
      final resp = await http.Response.fromStream(streamedResp);
      if (resp.statusCode == 201 || resp.statusCode == 200) {
        _showSnack('Uploaded successfully');
        _fetchBlocks();
      } else {
        throw Exception('Server returned ${resp.statusCode}');
      }
    } catch (e) {
      debugPrint('Upload failed, queuing offline: $e');
      final key = DateTime.now().millisecondsSinceEpoch.toString();
      await _offlineQueue.put(key, jsonEncode(payload));
      _showSnack('Saved offline — will sync when connected');
    }
  }

  Future<void> _flushOfflineQueue() async {
    final keys = _offlineQueue.keys.toList();
    for (final key in keys) {
      try {
        final raw = _offlineQueue.get(key);
        if (raw == null) continue;
        final Map<String, dynamic> payload = jsonDecode(raw as String);

        final uri = Uri.parse('$baseUrl/api/blocks');
        final request = http.MultipartRequest('POST', uri);
        request.fields['raw_content'] = payload['raw_content'] ?? '';
        request.fields['space'] = payload['space'] ?? 'Unfiled';
        request.fields['annotation'] = payload['annotation'] ?? '';

        final fp = payload['file_path'];
        if (fp != null && fp.toString().isNotEmpty) {
          final file = File(fp);
          if (await file.exists()) {
            request.files.add(
              await http.MultipartFile.fromPath('file', fp),
            );
          }
        }

        final streamedResp = await request.send().timeout(
              const Duration(seconds: 60),
            );
        if (streamedResp.statusCode == 201 || streamedResp.statusCode == 200) {
          await _offlineQueue.delete(key);
        }
      } catch (e) {
        debugPrint('Queue flush item failed: $e');
        break;
      }
    }
    if (keys.isNotEmpty) {
      _fetchBlocks();
    }
  }

  Future<void> _fetchBlocks() async {
    setState(() => _isLoading = true);
    try {
      final params = <String, String>{};
      if (_searchQuery.isNotEmpty) params['q'] = _searchQuery;
      if (_currentSpace != 'All') params['space'] = _currentSpace;

      final uri =
          Uri.parse('$baseUrl/api/blocks').replace(queryParameters: params);
      final resp = await http.get(uri).timeout(const Duration(seconds: 5));

      if (resp.statusCode == 200) {
        final List<dynamic> data = jsonDecode(resp.body);
        setState(() {
          _blocks = data.cast<Map<String, dynamic>>();
          _isLoading = false;
        });
        _cacheBlocks();
      } else {
        setState(() => _isLoading = false);
      }
    } catch (e) {
      debugPrint('Fetch blocks error: $e');
      setState(() => _isLoading = false);
    }
  }

  void _connectWebSocket() {
    _wsSub?.cancel();
    _wsChannel?.sink.close();
    _wsReconnectTimer?.cancel();

    try {
      _wsChannel = IOWebSocketChannel.connect(Uri.parse(wsUrl));
      setState(() => _wsConnected = true);

      _wsSub = _wsChannel!.stream.listen(
        (message) {
          try {
            final event = jsonDecode(message as String);
            if (event['type'] == 'BLOCK_CREATED' ||
                event['type'] == 'BLOCK_UPDATED' ||
                event['type'] == 'BLOCK_DELETED') {
              _fetchBlocks();
              _flushOfflineQueue();
            }
          } catch (_) {}
        },
        onDone: () {
          setState(() => _wsConnected = false);
          _scheduleReconnect();
        },
        onError: (err) {
          setState(() => _wsConnected = false);
          _scheduleReconnect();
        },
      );
    } catch (e) {
      debugPrint('WebSocket connect error: $e');
      setState(() => _wsConnected = false);
      _scheduleReconnect();
    }
  }

  void _scheduleReconnect() {
    _wsReconnectTimer?.cancel();
    _wsReconnectTimer = Timer(const Duration(seconds: 4), _connectWebSocket);
  }

  String _fileUrl(Map<String, dynamic> block, {bool download = false}) {
    final filePath = (block['file_path'] ?? '').toString();
    final fileName = (block['file_name'] ?? '').toString();
    return Uri.parse('$baseUrl/api/file').replace(queryParameters: {
      'path': filePath,
      if (fileName.isNotEmpty) 'name': fileName,
      if (download) 'download': '1',
    }).toString();
  }

  Future<void> _openBlock(Map<String, dynamic> block) async {
    final type = (block['block_type'] ?? 'text') as String;
    final rawContent = (block['raw_content'] ?? '').toString().trim();
    final filePath = (block['file_path'] ?? '').toString();
    final fileName = (block['file_name'] ?? '').toString();
    final mimeType = (block['mime_type'] ?? '').toString();
    final id = block['id'] ?? 0;

    if (type == 'link' ||
        rawContent.startsWith('http://') ||
        rawContent.startsWith('https://')) {
      final uri = Uri.tryParse(rawContent);
      if (uri == null) {
        _showSnack('Invalid link');
        return;
      }
      final ok = await launchUrl(uri, mode: LaunchMode.externalApplication);
      if (!ok) _showSnack('No app found to open this link');
      return;
    }

    if (filePath.isEmpty) {
      if (rawContent.isNotEmpty) {
        await Clipboard.setData(ClipboardData(text: rawContent));
        _showSnack('Copied to clipboard');
      }
      return;
    }

    _showSnack('Opening with default app...');
    try {
      final dir = await getTemporaryDirectory();
      String ext = '';
      if (fileName.contains('.')) {
        ext = fileName.split('.').last;
      } else if (filePath.contains('.')) {
        ext = filePath.split('.').last;
      }

      var rawName = fileName.isNotEmpty ? fileName : 'file_$id';
      var safeName = rawName.replaceAll(RegExp(r'[\\/:*?"<>|]'), '_');
      if (!safeName.contains('.') && ext.isNotEmpty) {
        safeName = '$safeName.$ext';
      }

      final local = File('${dir.path}/${id}_$safeName');
      if (!await local.exists()) {
        final resp = await http
            .get(Uri.parse(_fileUrl(block)))
            .timeout(const Duration(seconds: 45));
        if (resp.statusCode != 200) {
          _showSnack('Could not download file from server');
          return;
        }
        await local.writeAsBytes(resp.bodyBytes, flush: true);
      }

      OpenResult result;
      final cleanMime = (mimeType.isNotEmpty && mimeType != 'application/octet-stream')
          ? mimeType
          : null;

      if (cleanMime != null) {
        result = await OpenFilex.open(local.path, type: cleanMime);
        if (result.type != ResultType.done) {
          result = await OpenFilex.open(local.path);
        }
      } else {
        result = await OpenFilex.open(local.path);
      }

      if (result.type != ResultType.done) {
        try {
          final uri = Uri.file(local.path);
          final ok = await launchUrl(uri, mode: LaunchMode.externalApplication);
          if (!ok) {
            _showSnack(result.message.isNotEmpty
                ? result.message
                : 'No app found to open this file');
          }
        } catch (_) {
          _showSnack(result.message.isNotEmpty
              ? result.message
              : 'No app found to open this file');
        }
      }
    } catch (e) {
      debugPrint('Open block error: $e');
      _showSnack('Could not open file: $e');
    }
  }

  Future<void> _downloadBlock(Map<String, dynamic> block) async {
    final filePath = (block['file_path'] ?? '').toString();
    final fileName = (block['file_name'] ?? '').toString();
    final id = block['id'] ?? 0;

    if (filePath.isEmpty) {
      _showSnack('No file to download');
      return;
    }

    _showSnack('Downloading...');

    // 1. Try launching with system downloader / default browser
    try {
      final dlUrl = _fileUrl(block, download: true);
      final launched = await launchUrl(
        Uri.parse(dlUrl),
        mode: LaunchMode.externalApplication,
      );
      if (launched) {
        _showSnack('Download started in default browser');
        return;
      }
    } catch (_) {}

    // 2. Fallback: download bytes and save to device
    try {
      final resp = await http
          .get(Uri.parse(_fileUrl(block, download: true)))
          .timeout(const Duration(seconds: 60));
      if (resp.statusCode != 200) {
        _showSnack('Download failed: server returned ${resp.statusCode}');
        return;
      }

      Directory? saveDir;
      try {
        final publicDl = Directory('/storage/emulated/0/Download');
        if (await publicDl.exists()) {
          saveDir = publicDl;
        }
      } catch (_) {}

      saveDir ??= await getExternalStorageDirectory() ??
          await getApplicationDocumentsDirectory();

      String ext = '';
      if (fileName.contains('.')) {
        ext = fileName.split('.').last;
      } else if (filePath.contains('.')) {
        ext = filePath.split('.').last;
      }
      var safeName = (fileName.isNotEmpty ? fileName : 'streamspace_$id')
          .replaceAll(RegExp(r'[\\/:*?"<>|]'), '_');
      if (!safeName.contains('.') && ext.isNotEmpty) {
        safeName = '$safeName.$ext';
      }

      final targetFile = File('${saveDir.path}/$safeName');
      await targetFile.writeAsBytes(resp.bodyBytes, flush: true);

      _showSnack('Saved: $safeName');
    } catch (e) {
      debugPrint('Download error: $e');
      _showSnack('Download error: $e');
    }
  }

  Future<void> _deleteBlock(int id) async {
    try {
      final resp = await http
          .delete(Uri.parse('$baseUrl/api/blocks/delete?id=$id'))
          .timeout(const Duration(seconds: 3));
      if (resp.statusCode == 200) {
        _showSnack('Block deleted');
      }
    } catch (e) {
      _showSnack('Delete failed');
    }
  }

  void _showSnack(String msg) {
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(msg, style: const TextStyle(fontSize: 13)),
        behavior: SnackBarBehavior.floating,
        backgroundColor: kBgCard,
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(12)),
        duration: const Duration(seconds: 2),
        margin: const EdgeInsets.all(16),
      ),
    );
  }

  IconData _typeIcon(String type) {
    switch (type) {
      case 'image':
        return Icons.image_rounded;
      case 'pdf':
        return Icons.picture_as_pdf_rounded;
      case 'link':
        return Icons.link_rounded;
      case 'code':
        return Icons.code_rounded;
      default:
        return Icons.text_snippet_rounded;
    }
  }

  Color _typeColor(String type) {
    switch (type) {
      case 'image':
        return const Color(0xFFF472B6);
      case 'pdf':
        return const Color(0xFFFBBF24);
      case 'link':
        return const Color(0xFF60A5FA);
      case 'code':
        return const Color(0xFF22D3EE);
      default:
        return const Color(0xFF4ADE80);
    }
  }

  Widget _buildBlockCard(Map<String, dynamic> block) {
    final type = (block['block_type'] ?? 'text') as String;
    final rawContent = (block['raw_content'] ?? '') as String;
    final fileName = (block['file_name'] ?? '') as String;
    final filePath = (block['file_path'] ?? '') as String;
    final space = (block['space'] ?? 'Unfiled') as String;
    final extracted = (block['extracted_content'] ?? '') as String;
    final annotation = (block['annotation'] ?? '') as String;
    final id = (block['id'] ?? 0) as int;
    final fileSize = (block['file_size'] ?? 0);
    final createdAt = (block['created_at'] ?? '') as String;
    final hasFile = filePath.isNotEmpty;
    final isLink = type == 'link';
    final isOpenable = hasFile || isLink;

    final displayTitle =
        fileName.isNotEmpty ? fileName : rawContent.split('\n').first;
    final preview = extracted.isNotEmpty ? extracted : rawContent;

    final tColor = _typeColor(type);

    String timeLabel = '';
    if (createdAt.isNotEmpty) {
      try {
        final dt = DateTime.parse(createdAt);
        final diff = DateTime.now().difference(dt);
        if (diff.inMinutes < 1) {
          timeLabel = 'just now';
        } else if (diff.inHours < 1) {
          timeLabel = '${diff.inMinutes}m ago';
        } else if (diff.inDays < 1) {
          timeLabel = '${diff.inHours}h ago';
        } else {
          timeLabel = '${diff.inDays}d ago';
        }
      } catch (_) {}
    }

    String sizeLabel = '';
    if (fileSize is int && fileSize > 0) {
      if (fileSize < 1024) {
        sizeLabel = '$fileSize B';
      } else if (fileSize < 1048576) {
        sizeLabel = '${(fileSize / 1024).toStringAsFixed(1)} KB';
      } else {
        sizeLabel = '${(fileSize / 1048576).toStringAsFixed(1)} MB';
      }
    }

    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 6),
      decoration: BoxDecoration(
        color: kBgCard.withValues(alpha: 0.7),
        borderRadius: BorderRadius.circular(18),
        border: Border.all(color: Colors.white.withValues(alpha: 0.04)),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withValues(alpha: 0.2),
            blurRadius: 12,
            offset: const Offset(0, 4),
          ),
        ],
      ),
      child: Material(
        color: Colors.transparent,
        borderRadius: BorderRadius.circular(18),
        child: InkWell(
          borderRadius: BorderRadius.circular(18),
          onTap: isOpenable ? () => _openBlock(block) : null,
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    Container(
                      width: 36,
                      height: 36,
                      decoration: BoxDecoration(
                        color: tColor.withValues(alpha: 0.15),
                        borderRadius: BorderRadius.circular(10),
                      ),
                      child: Icon(_typeIcon(type), size: 18, color: tColor),
                    ),
                    const SizedBox(width: 12),
                    Expanded(
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(
                            displayTitle.length > 60
                                ? '${displayTitle.substring(0, 60)}...'
                                : displayTitle,
                            style: const TextStyle(
                              fontSize: 14,
                              fontWeight: FontWeight.w600,
                              color: Colors.white,
                            ),
                            maxLines: 1,
                            overflow: TextOverflow.ellipsis,
                          ),
                          const SizedBox(height: 3),
                          Row(
                            children: [
                              Container(
                                padding: const EdgeInsets.symmetric(
                                    horizontal: 7, vertical: 2),
                                decoration: BoxDecoration(
                                  color: tColor.withValues(alpha: 0.12),
                                  borderRadius: BorderRadius.circular(6),
                                ),
                                child: Text(
                                  type.toUpperCase(),
                                  style: TextStyle(
                                    fontSize: 9,
                                    fontWeight: FontWeight.w700,
                                    color: tColor,
                                    letterSpacing: 0.5,
                                  ),
                                ),
                              ),
                              if (timeLabel.isNotEmpty) ...[
                                const SizedBox(width: 8),
                                Text(
                                  timeLabel,
                                  style: TextStyle(
                                      fontSize: 11,
                                      color: Colors.grey.shade600),
                                ),
                              ],
                              if (sizeLabel.isNotEmpty) ...[
                                const SizedBox(width: 8),
                                Text(
                                  sizeLabel,
                                  style: TextStyle(
                                      fontSize: 11,
                                      color: Colors.grey.shade600),
                                ),
                              ],
                            ],
                          ),
                        ],
                      ),
                    ),
                    PopupMenuButton<String>(
                      icon: Icon(Icons.more_vert_rounded,
                          size: 18, color: Colors.grey.shade600),
                      color: kBgSurface,
                      shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(14)),
                      onSelected: (val) {
                        if (val == 'delete') {
                          _deleteBlock(id);
                        } else if (val == 'copy') {
                          Clipboard.setData(
                              ClipboardData(text: rawContent));
                          _showSnack('Copied');
                        } else if (val == 'open') {
                          _openBlock(block);
                        } else if (val == 'download') {
                          _downloadBlock(block);
                        }
                      },
                      itemBuilder: (ctx) => [
                        if (isOpenable)
                          const PopupMenuItem(
                            value: 'open',
                            child: Row(
                              children: [
                                Icon(Icons.open_in_new_rounded,
                                    size: 16, color: kAccentLight),
                                SizedBox(width: 10),
                                Text('Open',
                                    style: TextStyle(fontSize: 13)),
                              ],
                            ),
                          ),
                        if (hasFile)
                          const PopupMenuItem(
                            value: 'download',
                            child: Row(
                              children: [
                                Icon(Icons.download_rounded,
                                    size: 16, color: kAccentLight),
                                SizedBox(width: 10),
                                Text('Download',
                                    style: TextStyle(fontSize: 13)),
                              ],
                            ),
                          ),
                        const PopupMenuItem(
                          value: 'copy',
                          child: Row(
                            children: [
                              Icon(Icons.copy_rounded,
                                  size: 16, color: kAccentLight),
                              SizedBox(width: 10),
                              Text('Copy',
                                  style: TextStyle(fontSize: 13)),
                            ],
                          ),
                        ),
                        const PopupMenuItem(
                          value: 'delete',
                          child: Row(
                            children: [
                              Icon(Icons.delete_outline_rounded,
                                  size: 16, color: Colors.redAccent),
                              SizedBox(width: 10),
                              Text('Delete',
                                  style: TextStyle(
                                      fontSize: 13,
                                      color: Colors.redAccent)),
                            ],
                          ),
                        ),
                      ],
                    ),
                  ],
                ),
                const SizedBox(height: 10),
                Container(
                  padding:
                      const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
                  decoration: BoxDecoration(
                    color: kAccent.withValues(alpha: 0.1),
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Text(
                    space,
                    style: const TextStyle(
                      fontSize: 11,
                      fontWeight: FontWeight.w600,
                      color: kAccentLight,
                    ),
                  ),
                ),
                if (type == 'image' && hasFile) ...[
                  const SizedBox(height: 10),
                  GestureDetector(
                    onTap: () => _openBlock(block),
                    child: ClipRRect(
                      borderRadius: BorderRadius.circular(12),
                      child: Stack(
                        alignment: Alignment.bottomRight,
                        children: [
                          Image.network(
                            _fileUrl(block),
                            height: 180,
                            width: double.infinity,
                            fit: BoxFit.cover,
                            errorBuilder: (_, __, ___) => Container(
                              height: 90,
                              alignment: Alignment.center,
                              color: Colors.white.withValues(alpha: 0.03),
                              child: Column(
                                mainAxisAlignment: MainAxisAlignment.center,
                                children: [
                                  Icon(Icons.broken_image_rounded,
                                      size: 24, color: Colors.grey.shade600),
                                  const SizedBox(height: 4),
                                  Text(
                                    'Tap to open image',
                                    style: TextStyle(
                                        fontSize: 12, color: Colors.grey.shade500),
                                  ),
                                ],
                              ),
                            ),
                          ),
                          Container(
                            margin: const EdgeInsets.all(8),
                            padding: const EdgeInsets.symmetric(
                                horizontal: 8, vertical: 4),
                            decoration: BoxDecoration(
                              color: Colors.black.withValues(alpha: 0.65),
                              borderRadius: BorderRadius.circular(8),
                            ),
                            child: const Row(
                              mainAxisSize: MainAxisSize.min,
                              children: [
                                Icon(Icons.fullscreen_rounded,
                                    size: 14, color: Colors.white),
                                SizedBox(width: 4),
                                Text(
                                  'Tap to view',
                                  style: TextStyle(
                                      fontSize: 10,
                                      color: Colors.white,
                                      fontWeight: FontWeight.w500),
                                ),
                              ],
                            ),
                          ),
                        ],
                      ),
                    ),
                  ),
                ],
                if (hasFile) ...[
                  const SizedBox(height: 10),
                  Row(
                    children: [
                      Expanded(
                        child: OutlinedButton.icon(
                          icon: const Icon(Icons.open_in_new_rounded, size: 14),
                          label: const Text(
                            'Open',
                            style: TextStyle(fontSize: 12, fontWeight: FontWeight.w600),
                          ),
                          style: OutlinedButton.styleFrom(
                            foregroundColor: kAccentLight,
                            side: BorderSide(
                                color: kAccent.withValues(alpha: 0.35)),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(10),
                            ),
                            padding: const EdgeInsets.symmetric(vertical: 8),
                            visualDensity: VisualDensity.compact,
                          ),
                          onPressed: () => _openBlock(block),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: OutlinedButton.icon(
                          icon: const Icon(Icons.download_rounded, size: 14),
                          label: const Text(
                            'Download',
                            style: TextStyle(fontSize: 12, fontWeight: FontWeight.w600),
                          ),
                          style: OutlinedButton.styleFrom(
                            foregroundColor: Colors.white,
                            side: BorderSide(
                                color: Colors.white.withValues(alpha: 0.15)),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(10),
                            ),
                            padding: const EdgeInsets.symmetric(vertical: 8),
                            visualDensity: VisualDensity.compact,
                          ),
                          onPressed: () => _downloadBlock(block),
                        ),
                      ),
                    ],
                  ),
                ],
                if (preview.isNotEmpty && type != 'link' && type != 'image') ...[
                  const SizedBox(height: 10),
                  ExpandableText(
                    text: preview,
                    style: TextStyle(
                      fontSize: 13,
                      color: Colors.grey.shade400,
                      height: 1.45,
                    ),
                    collapsedLines: 4,
                  ),
                ],
                if (type == 'image' && extracted.isNotEmpty) ...[
                  const SizedBox(height: 10),
                  ExpandableText(
                    text: extracted,
                    style: TextStyle(
                      fontSize: 12,
                      color: Colors.grey.shade500,
                      height: 1.45,
                    ),
                    collapsedLines: 4,
                  ),
                ],
                if (type == 'link') ...[
                  const SizedBox(height: 8),
                  Row(
                    children: [
                      Icon(Icons.open_in_new_rounded,
                          size: 13, color: Colors.blue.shade400),
                      const SizedBox(width: 6),
                      Expanded(
                        child: Text(
                          rawContent,
                          style: TextStyle(
                            fontSize: 12,
                            color: Colors.blue.shade400,
                            decoration: TextDecoration.underline,
                          ),
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                    ],
                  ),
                ],
                if (annotation.isNotEmpty) ...[
                  const SizedBox(height: 8),
                  Row(
                    children: [
                      const Icon(Icons.chat_bubble_outline_rounded,
                          size: 12, color: kAccentLight),
                      const SizedBox(width: 6),
                      Expanded(
                        child: Text(
                          annotation,
                          style: TextStyle(
                            fontSize: 11,
                            color: kAccentLight.withValues(alpha: 0.8),
                            fontStyle: FontStyle.italic,
                          ),
                          maxLines: 2,
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                    ],
                  ),
                ],
              ],
            ),
          ),
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Row(
          children: [
            Container(
              width: 32,
              height: 32,
              decoration: BoxDecoration(
                gradient: const LinearGradient(
                  colors: [kAccent, kAccentDim],
                  begin: Alignment.topLeft,
                  end: Alignment.bottomRight,
                ),
                borderRadius: BorderRadius.circular(9),
                boxShadow: [
                  BoxShadow(
                    color: kAccent.withValues(alpha: 0.3),
                    blurRadius: 8,
                    offset: const Offset(0, 2),
                  ),
                ],
              ),
              child: const Icon(Icons.all_inbox_rounded,
                  size: 17, color: Colors.white),
            ),
            const SizedBox(width: 10),
            const Text('StreamSpace'),
          ],
        ),
        actions: [
          Container(
            width: 10,
            height: 10,
            margin: const EdgeInsets.only(right: 4),
            decoration: BoxDecoration(
              shape: BoxShape.circle,
              color: _wsConnected
                  ? const Color(0xFF4ADE80)
                  : const Color(0xFFEF4444),
              boxShadow: [
                BoxShadow(
                  color: (_wsConnected
                          ? const Color(0xFF4ADE80)
                          : const Color(0xFFEF4444))
                      .withValues(alpha: 0.5),
                  blurRadius: 6,
                ),
              ],
            ),
          ),
          Padding(
            padding: const EdgeInsets.only(right: 12),
            child: Text(
              _wsConnected ? 'Live' : 'Offline',
              style: TextStyle(
                fontSize: 11,
                fontWeight: FontWeight.w500,
                color: _wsConnected ? const Color(0xFF4ADE80) : Colors.grey,
              ),
            ),
          ),
        ],
        bottom: PreferredSize(
          preferredSize: const Size.fromHeight(108),
          child: Column(
            children: [
              Padding(
                padding:
                    const EdgeInsets.symmetric(horizontal: 16, vertical: 6),
                child: TextField(
                  controller: _searchController,
                  onChanged: (val) {
                    _searchQuery = val.trim();
                    _fetchBlocks();
                  },
                  decoration: InputDecoration(
                    hintText: 'Search blocks...',
                    prefixIcon: Icon(Icons.search_rounded,
                        size: 20, color: Colors.grey.shade600),
                    suffixIcon: _searchQuery.isNotEmpty
                        ? IconButton(
                            icon: Icon(Icons.clear_rounded,
                                size: 18, color: Colors.grey.shade500),
                            onPressed: () {
                              _searchController.clear();
                              _searchQuery = '';
                              _fetchBlocks();
                            },
                          )
                        : null,
                    isDense: true,
                  ),
                  style:
                      const TextStyle(fontSize: 14, color: Colors.white),
                ),
              ),
              SizedBox(
                height: 46,
                child: ListView(
                  scrollDirection: Axis.horizontal,
                  padding: const EdgeInsets.symmetric(horizontal: 12),
                  children: defaultSpaces.map((s) {
                    final isActive = _currentSpace == s;
                    return Padding(
                      padding: const EdgeInsets.symmetric(
                          horizontal: 4, vertical: 6),
                      child: ChoiceChip(
                        label: Text(
                          s,
                          style: TextStyle(
                            fontSize: 12,
                            fontWeight: FontWeight.w600,
                            color: isActive
                                ? Colors.white
                                : Colors.grey.shade500,
                          ),
                        ),
                        selected: isActive,
                        onSelected: (_) {
                          setState(() => _currentSpace = s);
                          _fetchBlocks();
                        },
                        backgroundColor: kBgCard,
                        selectedColor: kAccent,
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(10),
                          side: BorderSide(
                            color: isActive
                                ? kAccent
                                : Colors.white.withValues(alpha: 0.06),
                          ),
                        ),
                        elevation: isActive ? 4 : 0,
                        shadowColor: kAccent.withValues(alpha: 0.3),
                      ),
                    );
                  }).toList(),
                ),
              ),
            ],
          ),
        ),
      ),
      body: _isLoading && _blocks.isEmpty
          ? const Center(
              child: CircularProgressIndicator(color: kAccent),
            )
          : _blocks.isEmpty
              ? Center(
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      Container(
                        width: 64,
                        height: 64,
                        decoration: BoxDecoration(
                          color: kBgCard,
                          borderRadius: BorderRadius.circular(18),
                        ),
                        child: Icon(Icons.inbox_rounded,
                            size: 32, color: Colors.grey.shade700),
                      ),
                      const SizedBox(height: 16),
                      Text(
                        'No blocks yet',
                        style: TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.w600,
                          color: Colors.grey.shade500,
                        ),
                      ),
                      const SizedBox(height: 6),
                      Text(
                        'Share content from other apps or type below',
                        style: TextStyle(
                            fontSize: 12, color: Colors.grey.shade700),
                      ),
                    ],
                  ),
                )
              : RefreshIndicator(
                  color: kAccent,
                  backgroundColor: kBgCard,
                  onRefresh: _fetchBlocks,
                  child: ListView.builder(
                    controller: _scrollController,
                    padding: const EdgeInsets.only(top: 8, bottom: 100),
                    itemCount: _blocks.length,
                    itemBuilder: (ctx, i) => _buildBlockCard(_blocks[i]),
                  ),
                ),
      bottomSheet: Container(
        padding: const EdgeInsets.fromLTRB(16, 10, 16, 24),
        decoration: BoxDecoration(
          color: kBgSurface,
          border: Border(
            top: BorderSide(color: Colors.white.withValues(alpha: 0.04)),
          ),
          boxShadow: [
            BoxShadow(
              color: Colors.black.withValues(alpha: 0.3),
              blurRadius: 20,
              offset: const Offset(0, -4),
            ),
          ],
        ),
        child: SafeArea(
          top: false,
          child: Row(
            children: [
              Expanded(
                child: TextField(
                  controller: _noteController,
                  decoration: const InputDecoration(
                    hintText: 'Type a note or paste a URL...',
                    isDense: true,
                  ),
                  style: const TextStyle(fontSize: 14, color: Colors.white),
                  onSubmitted: (val) {
                    final text = val.trim();
                    if (text.isEmpty) return;
                    _sendOrQueue(rawContent: text);
                    _noteController.clear();
                  },
                ),
              ),
              const SizedBox(width: 10),
              Container(
                decoration: BoxDecoration(
                  gradient: const LinearGradient(
                    colors: [kAccent, kAccentDim],
                  ),
                  borderRadius: BorderRadius.circular(14),
                  boxShadow: [
                    BoxShadow(
                      color: kAccent.withValues(alpha: 0.3),
                      blurRadius: 10,
                      offset: const Offset(0, 3),
                    ),
                  ],
                ),
                child: IconButton(
                  icon: const Icon(Icons.send_rounded,
                      size: 20, color: Colors.white),
                  onPressed: () {
                    final text = _noteController.text.trim();
                    if (text.isEmpty) return;
                    _sendOrQueue(rawContent: text);
                    _noteController.clear();
                  },
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class ExpandableText extends StatefulWidget {
  final String text;
  final TextStyle? style;
  final int collapsedLines;
  final int characterLimit;

  const ExpandableText({
    super.key,
    required this.text,
    this.style,
    this.collapsedLines = 4,
    this.characterLimit = 180,
  });

  @override
  State<ExpandableText> createState() => _ExpandableTextState();
}

class _ExpandableTextState extends State<ExpandableText> {
  bool _expanded = false;

  @override
  Widget build(BuildContext context) {
    final effectiveStyle = widget.style ??
        TextStyle(
          fontSize: 13,
          color: Colors.grey.shade300,
          height: 1.45,
        );

    return LayoutBuilder(
      builder: (context, constraints) {
        final text = widget.text;
        final hasNewlineOverflow =
            text.split('\n').length > widget.collapsedLines;
        final hasCharOverflow = text.length > widget.characterLimit;

        // Measure with TextPainter to detect horizontal wrapping overflow
        final textSpan = TextSpan(text: text, style: effectiveStyle);
        final textPainter = TextPainter(
          text: textSpan,
          maxLines: widget.collapsedLines,
          textDirection: Directionality.maybeOf(context) ?? TextDirection.ltr,
        )..layout(
            maxWidth: constraints.maxWidth > 0
                ? constraints.maxWidth
                : double.infinity,
          );

        final isOverflowing = textPainter.didExceedMaxLines ||
            hasNewlineOverflow ||
            hasCharOverflow;

        if (!isOverflowing) {
          return SelectableText(
            text,
            style: effectiveStyle,
          );
        }

        return Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            InkWell(
              onTap: () => setState(() => _expanded = !_expanded),
              splashColor: Colors.transparent,
              highlightColor: Colors.transparent,
              child: AnimatedCrossFade(
                firstChild: Text(
                  text,
                  style: effectiveStyle,
                  maxLines: widget.collapsedLines,
                  overflow: TextOverflow.ellipsis,
                ),
                secondChild: SelectableText(
                  text,
                  style: effectiveStyle,
                ),
                crossFadeState: _expanded
                    ? CrossFadeState.showSecond
                    : CrossFadeState.showFirst,
                duration: const Duration(milliseconds: 200),
              ),
            ),
            const SizedBox(height: 4),
            GestureDetector(
              onTap: () => setState(() => _expanded = !_expanded),
              child: Padding(
                padding: const EdgeInsets.symmetric(vertical: 2),
                child: Row(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    Text(
                      _expanded ? 'Show less' : 'Read more',
                      style: const TextStyle(
                        fontSize: 13,
                        fontWeight: FontWeight.w600,
                        color: kAccentLight,
                      ),
                    ),
                    const SizedBox(width: 4),
                    Icon(
                      _expanded
                          ? Icons.keyboard_arrow_up_rounded
                          : Icons.keyboard_arrow_down_rounded,
                      size: 16,
                      color: kAccentLight,
                    ),
                  ],
                ),
              ),
            ),
          ],
        );
      },
    );
  }
}

