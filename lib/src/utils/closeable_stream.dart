import 'dart:async';

class CloseableStream<T> extends Stream<T> {
  StreamSubscription<T> _streamSubscription;

  final Stream<T> _stream;
  final void Function(T event) onData;
  final void Function() onDone;
  final Function onError;

  CloseableStream(
    this._stream, {
    this.onData,
    this.onDone,
    this.onError,
  });

  @override
  StreamSubscription<T> listen(
    void Function(T event) onData, {
    Function onError,
    void Function() onDone,
    bool cancelOnError,
  }) {
    assert(onData != null);

    void Function(T event) _onData;

    if (this.onData != null && onData != null) {
      _onData = (event) {
        this.onData(event);
        onData(event);
      };
    } else {
      _onData = onData ?? this.onData;
    }

    Function _onError;
    if (this.onError != null && onError != null) {
      _onError = (e) {
        this.onError(e);
        onError(e);
      };
    } else {
      _onError = onError ?? this.onError;
    }

    void Function() _onDone;
    if (this.onDone != null && onDone != null) {
      _onDone = () {
        this.onDone();
        onDone();
      };
    } else {
      _onDone = onDone ?? this.onDone;
    }

    try {
      return _stream.listen(
        _onData,
        onError: _onError,
        onDone: _onDone,
        cancelOnError: cancelOnError,
      );
    } catch (e) {
      _onError(e);
      rethrow;
    }
  }

  Future close() {
    return _streamSubscription?.cancel() ?? Future.value();
  }
}
