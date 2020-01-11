import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:meta/meta.dart';
import 'package:restio/src/cache/cache_store.dart';
import 'package:restio/src/cache/editor.dart';
import 'package:restio/src/cache/lru_map.dart';
import 'package:restio/src/cache/snapshot.dart';
import 'package:restio/src/utils/closeable_stream.dart';
import 'package:synchronized/synchronized.dart';

class DiskLruCacheStore implements CacheStore {
  static const magic = 'DiskLruCacheStore';
  static const version = '1';
  static const anySequenceNumber = -1;

  static const maxOpCount = 2000;

  final File _journalFile;
  final int opCompactThreshold;
  final File _journalFileTmp;
  final File _journalFileBackup;
  final Directory directory;
  final int maxSize;
  final int _filesCount;
  final LruMap<String, _Entry> _lruEntries;

  int _opCount = 0;

  bool _initialized = false;
  bool _closed = false;

  int _size = 0;

  final lock = Lock(reentrant: true);

  IOSink _journalWriter;

  int _sequenceNumber = 0;

  DiskLruCacheStore(
    this.directory, {
    this.maxSize = 20 * 1024 * 1024,
    int filesCount = 2,
    this.opCompactThreshold = maxOpCount,
  })  : assert(directory != null),
        _filesCount = filesCount,
        _lruEntries = LruMap(),
        _journalFile = File('${directory.path}/journal'),
        _journalFileTmp = File('${directory.path}/journal.tmp'),
        _journalFileBackup = File('${directory.path}/journal.bak');

  /// Returns a snapshot of the entry named key, or null if it doesn't exist is not currently
  /// readable. If a value is returned, it is moved to the tail of the LRU queue.
  @override
  Future<Snapshot> get(String key) {
    return lock.synchronized<_CacheSnapshot>(() async {
      await _lazyInit();

      final entry = _lruEntries[key];

      if (entry == null || !entry.ready) {
        return null;
      }

      final snapshot = await entry.snapshot();

      if (snapshot == null) {
        return null;
      }

      await _journalRead(key);

      return snapshot;
    });
  }

  Future clean() {
    return lock.synchronized(() async {
      final entries = await values;
      final list = <Future>[];

      for (final entry in entries) {
        list.add(remove(entry.key));
      }

      return Future.wait(list);
    });
  }

  @override
  Future<Editor> edit(
    String key, [
    int sequenceNumber = anySequenceNumber,
  ]) {
    return lock.synchronized<Editor>(() async {
      await _lazyInit();

      var entry = _lruEntries[key];

      if ((entry == null || entry.sequenceNumber != sequenceNumber) &&
          sequenceNumber != anySequenceNumber) {
        return null;
      }

      if (entry != null && entry.currentEditor != null) {
        return null; // Another edit is in progress.
      }

      // Flush the journal before creating files to prevent file leaks.
      await _journalDirty(key);

      if (entry == null) {
        entry = _Entry(
          key: key,
          cache: this,
        );
        _lruEntries[key] = entry;
      }

      final editor = _Editor._(entry: entry, cache: this);
      entry.currentEditor = editor;
      return editor;
    });
  }

  Future _journalRead(String key) async {
    ++_opCount;

    _journalWriter.write('READ $key\n');

    await _journalWriter.flush();

    if (_needsRebuild()) {
      await _cleanUp();
    }
  }

  Future _journalDirty(String key) async {
    ++_opCount;

    _journalWriter.write('DIRTY $key\n');

    await _journalWriter.flush();

    if (_needsRebuild()) {
      await _cleanUp();
    }
  }

  Future _journalClean(
    String key,
    List<int> lengths,
  ) async {
    ++_opCount;

    _journalWriter.write('CLEAN $key');

    for (final length in lengths) {
      _journalWriter.write(' $length');
    }

    _journalWriter.write('\n');

    await _journalWriter.flush();

    if (_needsRebuild() || _size > maxSize) {
      await _cleanUp();
    }
  }

  Future _journalRemove(String key) async {
    ++_opCount;
    _journalWriter.write('REMOVE $key\n');
    await _journalWriter.flush();
    if (_needsRebuild()) {
      await _cleanUp();
    }
  }

  // We only rebuild journal file when opCount is at least MAX_OP_COUNT.
  bool _needsRebuild() {
    return _opCount >= opCompactThreshold && _opCount >= _lruEntries.length;
  }

  Future _trimToSize() async {
    while (_size > maxSize) {
      final toEvict = _lruEntries.removeHead();
      await _removeEntry(toEvict);
    }
  }

  Future _cleanUp() {
    return lock.synchronized(() async {
      try {
        print('Start cleanup');
        await _trimToSize();
        if (_needsRebuild()) {
          await _rebuildRecord();
        }
        print('Cleanup success');
      } catch (e) {
        print('Cleanup failed! $e');
      }
    });
  }

  Future _rebuildRecord() {
    return lock.synchronized(() async {
      print('Start to rebuild journal');
      if (_journalWriter != null) {
        await _journalWriter.close();
      }

      if (!directory.existsSync()) {
        await directory.create(recursive: true);
      }

      final writer = _journalFileTmp.openWrite();

      try {
        writer.write('$magic\n$version\n$_filesCount\n\n');

        for (final entry in _lruEntries.values) {
          entry._writeTo(writer);
        }

        await writer.flush();
      } catch (e) {
        print('Cannot write file at this time $e');
        return;
      } finally {
        try {
          await writer.close();
        } catch (e) {
          print('Cannot write file at this time $e');
          return;
        }
      }

      if (_journalFile.existsSync()) {
        _journalFile.renameSync(_journalFileBackup.path);
      }

      _journalFileTmp.renameSync(_journalFile.path);
      _deleteSafe(_journalFileBackup);

      _journalWriter = _newRecordWriter();

      print('Rebuild journal success!');
    });
  }

  IOSink _newRecordWriter() {
    return _journalFile.openWrite(mode: FileMode.append);
  }

  Future _lazyInit() {
    return lock.synchronized(() async {
      if (_initialized) {
        return null;
      }

      if (!directory.existsSync()) {
        await directory.create(recursive: true);
      }

      // If a bkp file exists, use it instead.
      if (_journalFileBackup.existsSync()) {
        // If recod file also exists just delete backup file.
        if (_journalFile.existsSync()) {
          await _journalFileBackup.delete();
        } else {
          _journalFileBackup.renameSync(_journalFile.path);
        }
      }

      if (_journalFile.existsSync()) {
        try {
          await _parseRecordFile();
          await _processRecords();
          _initialized = true;
          return null;
        } catch (e) {
          try {
            await _deleteCache();
          } catch (e) {
            // nada.
          }
        }
      }
      await _rebuildRecord();
      _initialized = true;
    });
  }

  Future _deleteCache() async {
    await close();
    await directory.delete(recursive: true);
  }

  Future<Iterable<_Entry>> get values {
    return lock.synchronized(() async {
      await _lazyInit();
      return List<_Entry>.from(_lruEntries.values);
    });
  }

  Future _parseRecordFile() async {
    try {
      final lines = await _journalFile.readAsLines();

      if (lines.length < 4) {
        throw Exception('The journal file is broken: Too small to parse');
      }

      final magic = lines[0];
      final version = lines[1];
      final filesCountString = lines[2];
      final blank = lines[3];

      if (magic != DiskLruCacheStore.magic ||
          version != DiskLruCacheStore.version ||
          filesCountString != _filesCount.toString() ||
          blank != '') {
        throw Exception(
            'The journal file is broken: unexpected file header:[$magic,$version,$filesCountString,$blank]');
      }

      var lineCount = 0;

      for (var i = 4; i < lines.length; i++) {
        _parseRecordLine(lines[i]);
        lineCount++;
      }

      _opCount = lineCount - _lruEntries.length;

      _journalWriter = _newRecordWriter();
    } catch (e) {
      print(e);
      rethrow;
    }
  }

  void _parseRecordLine(String line) {
    final firstSpace = line.indexOf(' ');

    if (firstSpace == -1) {
      throw Exception('unexpected journal line: $line');
    }

    final keyBegin = firstSpace + 1;
    final secondSpace = line.indexOf(' ', keyBegin);
    String key;

    if (secondSpace == -1) {
      key = line.substring(keyBegin);
      if (firstSpace == 6 && line.startsWith('REMOVE')) {
        _lruEntries.remove(key);
        return;
      }
    } else {
      key = line.substring(keyBegin, secondSpace);
    }

    var entry = _lruEntries[key];

    if (entry == null) {
      entry = _Entry(
        key: key,
        cache: this,
      );
      _lruEntries[key] = entry;
    }

    if (secondSpace != -1 && firstSpace == 5 && line.startsWith('CLEAN')) {
      final parts = line.substring(secondSpace + 1).split(' ');
      entry.ready = true;
      entry.currentEditor = null;
      entry.setLengths(parts.map(int.parse).toList());
    } else if (secondSpace == -1 &&
        firstSpace == 5 &&
        line.startsWith('DIRTY')) {
      entry.currentEditor = _Editor._(entry: entry, cache: this);
    } else if (secondSpace == -1 &&
        firstSpace == 4 &&
        line.startsWith('READ')) {
      // This work was already done by calling lruEntries.get().
    } else {
      throw Exception('unexpected journal line: $line');
    }
  }

  /// Close the cache, do some clean stuff, it is an error to use cache when cache is closed.
  Future close() {
    return lock.synchronized(() async {
      if (_closed) {
        return null;
      }

      try {
        if (_journalWriter != null) {
          await _journalWriter.close();
          _journalWriter = null;
        }
      } finally {
        _closed = true;
        _initialized = false;
      }

      return null;
    });
  }

  @override
  Future<bool> remove(String key) {
    return lock.synchronized<bool>(() async {
      await _lazyInit();
      final entry = _lruEntries[key];
      if (entry == null) return false;
      await _removeEntry(entry);
      return true;
    });
  }

  /// Error when read the cache stream, the cache must be removed
  void _onCacheReadError(String key, e) {
    remove(key);
  }

  Future _processRecords() async {
    _deleteSafe(_journalFileTmp);

    final list = List.of(_lruEntries.values);
    var size = 0;

    for (final entry in list) {
      if (entry.currentEditor == null) {
        for (var t = 0; t < _filesCount; t++) {
          size += entry.lengths[t];
        }
      } else {
        entry.currentEditor = null;

        for (var t = 0; t < _filesCount; t++) {
          _deleteSafe(entry.cleanFiles[t]);
          _deleteSafe(entry.dirtyFiles[t]);
        }

        _lruEntries.remove(entry.key);
      }
    }

    _size = size;
  }

  void _deleteSafe(File file) {
    if (file.existsSync()) {
      try {
        file.deleteSync();
      } catch (e) {
        // nada.
      }
    }
  }

  /// clean the entry, remove from cache.
  Future _rollback(_Editor editor) {
    return lock.synchronized(() async {
      final entry = editor.entry;
      entry.currentEditor = null;

      entry.dirtyFiles.forEach(_deleteSafe);

      if (entry.ready) {
        await _journalClean(entry.key, entry.lengths);
      } else {
        await _journalRemove(entry.key);
      }
    });
  }

  Future _complete(
    _Editor editor,
    bool success,
  ) async {
    try {
      await _commit(editor);
    } catch (e) {
      print('Error $e when commit $editor');
      await _rollback(editor);
    }
  }

  Future _commit(_Editor editor) {
    return lock.synchronized(() async {
      final entry = editor.entry;

      if (entry.currentEditor != editor) {
        throw Exception('Commit editor\'s entry did not match the editor');
      }

      if (!entry.ready) {
        if (!editor.hasValues.every((value) => value)) {
          await _rollback(editor);
          return null;
        }

        for (final file in editor.entry.dirtyFiles) {
          if (!file.existsSync()) {
            await _rollback(editor);
            return null;
          }
        }
      }

      var index = 0;

      for (final dirty in editor.entry.dirtyFiles) {
        final clean = entry.cleanFiles[index];
        await dirty.rename(clean.path);
        final oldLength = entry.lengths[index];
        final newLength = await clean.length();
        entry.lengths[index] = newLength;
        _size = _size - oldLength + newLength;
        index++;
      }

      entry.sequenceNumber = _sequenceNumber++;

      entry.ready = true;
      entry.currentEditor = null;

      await _journalClean(entry.key, entry.lengths);
    });
  }

  Future _removeEntry(_Entry entry) async {
    if (entry.currentEditor != null) {
      // Prevent the edit from completing normally.
      await entry.currentEditor.detach();
    }

    for (var i = 0; i < _filesCount; i++) {
      _deleteSafe(entry.cleanFiles[i]);
      _size -= entry.lengths[i];
      entry.lengths[i] = 0;
    }

    await _journalRemove(entry.key);

    return true;
  }

  @override
  Future<bool> clear() async {
    await Future.wait(_lruEntries.values.map(_removeEntry));
    _lruEntries.clear();
    return true;
  }

  @override
  Future<int> size() {
    return Future.value(_size);
  }
}

class _Editor implements Editor {
  final _Entry entry;

  final DiskLruCacheStore cache;

  // If a cache is first created, it must has value for all of the files.
  final List<bool> hasValues;

  bool _done = false;

  final Lock lock = Lock();

  _Editor._({
    @required this.entry,
    @required this.cache,
  })  : assert(entry != null),
        assert(cache != null),
        hasValues = List.filled(cache._filesCount, false);

  Future detach() {
    return lock.synchronized(() async {
      if (entry.currentEditor == this) {
        for (var i = 0; i < cache._filesCount; i++) {
          cache._deleteSafe(entry.dirtyFiles[i]);
        }

        entry.currentEditor = null;
      }
    });
  }

  @override
  String toString() {
    return 'Editor {key: ${entry.key}, done: $_done}';
  }

  @override
  Future abort() async {
    return cache.lock.synchronized(() async {
      if (_done) {
        return;
      }

      if (entry.currentEditor == this) {
        await cache._complete(this, false);
      }

      _done = true;
    });
  }

  @override
  Future commit() async {
    return cache.lock.synchronized(() async {
      if (_done) {
        return;
      }

      if (entry.currentEditor == this) {
        await cache._complete(this, true);
      }

      _done = true;
    });
  }

  @override
  Stream<List<int>> newSource(
    int index,
  ) {
    final sink = newSink(index);

    return CloseableStream(
      entry.dirtyFiles[index].openRead(),
      onData: sink.add,
      onDone: sink.close,
      onError: sink.addError,
    );
  }

  @override
  StreamSink<List<int>> newSink(int index) {
    if (_done) {
      throw Exception('The editor is finish done it\'s job');
    }

    if (entry.currentEditor != this) {
      return _EmptyIOSink();
    }

    if (!entry.ready) {
      hasValues[index] = true;
    }

    final dirtyFile = entry.dirtyFiles[index];

    return dirtyFile.openWrite();
  }
}

class _Entry {
  final List<File> cleanFiles;
  final List<File> dirtyFiles;
  final List<int> lengths;

  _Editor currentEditor;

  bool ready = false;

  final String key;
  final DiskLruCacheStore cache;

  int sequenceNumber;

  _Entry({
    this.key,
    this.cache,
    this.sequenceNumber,
  })  : cleanFiles = List(cache._filesCount),
        dirtyFiles = List(cache._filesCount),
        lengths = List(cache._filesCount) {
    // The names are repetitive so re-use the same builder to avoid allocations.
    for (var i = 0; i < cache._filesCount; i++) {
      cleanFiles[i] = File('${cache.directory.path}/$key.$i');
      dirtyFiles[i] = File('${cache.directory.path}/$key.$i.tmp');

      lengths[i] = 0;
    }
  }

  int get size {
    var _size = 0;

    for (var i = 0; i < lengths.length; i++) {
      _size += lengths[i];
    }

    return _size;
  }

  @override
  String toString() {
    return 'Entry { key: $key }';
  }

  void _onStreamError(e) {
    cache._onCacheReadError(key, e);
  }

  void setLengths(List<int> lengths) {
    List.writeIterable(this.lengths, 0, lengths);
  }

  Future<Snapshot> snapshot() async {
    final filesCount = cache._filesCount;

    final streams = List<CloseableStream<List<int>>>(filesCount);

    for (var i = 0; i < filesCount; i++) {
      if (cleanFiles[i].existsSync()) {
        try {
          streams[i] = CloseableStream(
            cleanFiles[i].openRead(),
            onError: _onStreamError,
          );
        } catch (e) {
          print('Open file read error $e');
          return null;
        }
      } else {
        cleanFiles.forEach(cache._deleteSafe);
        //File not found,then the cache is not exists,remove this cache
        cache._onCacheReadError(
          key,
          FileSystemException('File [${cleanFiles[i]}] not found'),
        );

        return null;
      }
    }
    try {
      return _CacheSnapshot(
        streams: streams,
        lengths: lengths,
        key: key,
        sequenceNumber: sequenceNumber,
      );
    } catch (e) {
      print(e);
      rethrow;
    }
  }

  void _writeTo(IOSink writer) {
    if (currentEditor != null) {
      writer.write('DIRTY $key\n');
    } else {
      writer.write('CLEAN $key');

      for (final length in lengths) {
        writer.write(' $length');
      }

      writer.write('\n');
    }
  }
}

class _CacheSnapshot implements Snapshot {
  final List<CloseableStream<List<int>>> streams;
  final List<int> lengths;
  @override
  final String key;
  @override
  final int sequenceNumber;

  _CacheSnapshot({
    this.key,
    this.sequenceNumber,
    @required List<CloseableStream<List<int>>> streams,
    this.lengths,
  })  : assert(
            streams != null && streams.isNotEmpty, 'Streams is null or empty'),
        streams = List.unmodifiable(streams);

  Future<Uint8List> getBytes(int index) async {
    final completer = Completer<Uint8List>();

    final sink = ByteConversionSink.withCallback(
      (bytes) => completer.complete(Uint8List.fromList(bytes)),
    );

    source(index).listen(
      sink.add,
      onError: completer.completeError,
      onDone: sink.close,
      cancelOnError: true,
    );

    return completer.future;
  }

  @override
  String toString() {
    return 'Snapshot { count: ${lengths.length}, key: $key }';
  }

  @override
  Stream<List<int>> source(int index) {
    assert(index >= 0 && index < streams.length);
    return streams[index];
  }

  @override
  Future close() {
    final list = <Future>[];

    for (final stream in streams) {
      list.add(stream.close());
    }

    return Future.wait(list);
  }

  @override
  int length(int index) {
    return lengths[index];
  }

  @override
  int size() {
    throw UnimplementedError();
  }
}

class _EmptyIOSink implements IOSink {
  @override
  Encoding encoding;

  @override
  void add(List<int> data) {}

  @override
  void addError(
    Object error, [
    StackTrace stackTrace,
  ]) {}

  @override
  Future addStream(Stream<List<int>> stream) {
    return Future.value();
  }

  @override
  Future close() {
    return Future.value();
  }

  @override
  Future get done => Future.value();

  @override
  Future flush() {
    return Future.value();
  }

  @override
  void write(Object obj) {}

  @override
  void writeAll(
    Iterable objects, [
    String separator = '',
  ]) {}

  @override
  void writeCharCode(int charCode) {}

  @override
  void writeln([Object obj = '']) {}
}
