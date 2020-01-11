class LruMap<K, V> implements Map<K, V> {
  _Entry<K, V> head;
  _Entry<K, V> tail;

  final _inner = <K, _Entry<K, V>>{};

  LruMap();

  factory LruMap.of(Map<K, V> map) {
    if (map is LruMap<K, V>) {
      return map;
    }

    final result = LruMap<K, V>();
    result.addAll(map);
    return result;
  }

  @override
  int get length => _inner.length;

  @override
  Iterable<V> get values {
    return <V>[for (var e = head; e != null; e = e.after) e.value];
  }

  @override
  Iterable<K> get keys {
    return <K>[for (var e = head; e != null; e = e.after) e.key];
  }

  @override
  void clear() {
    _inner.clear();
    head = tail = null;
  }

  @override
  V operator [](Object key) {
    final node = _inner[key];

    if (node == null) {
      return null;
    }

    _afterNodeAccess(node);

    return node.value;
  }

  void _afterNodeRemoval(_Entry<K, V> e) {
    final p = e, b = p.before, a = p.after;

    p.before = p.after = null;

    if (b == null) {
      head = a;
    } else {
      b.after = a;
    }

    if (a == null) {
      tail = b;
    } else {
      a.before = b;
    }
  }

  @override
  V remove(Object key) {
    final node = _inner.remove(key);

    if (node == null) {
      return null;
    }

    _afterNodeRemoval(node);

    return node.value;
  }

  void _linkNodeLast(_Entry<K, V> p) {
    final last = tail;

    tail = p;

    if (last == null) {
      head = p;
    } else {
      p.before = last;
      last.after = p;
    }
  }

  V removeHead() {
    final head = this.head;
    if (head == null) {
      return null;
    }
    if (head == tail) {
      //just one
      head.before = head.after = null;
      this.head = tail = null;
    } else {
      this.head = head.after;
      this.head.before = null;
      head.after = null;
    }

    _inner.remove(head.key);

    return head.value;
  }

  void _afterNodeAccess(_Entry<K, V> e) {
    _Entry<K, V> last;

    if ((last = tail) != e) {
      final p = e, b = p.before, a = p.after;

      p.after = null;

      if (b == null) {
        head = a;
      } else {
        b.after = a;
      }

      if (a != null) {
        a.before = b;
      } else {
        last = b;
      }

      if (last == null) {
        head = p;
      } else {
        p.before = last;
        last.after = p;
      }

      tail = p;
    }
  }

  @override
  void addAll(Map<K, V> other) {
    assert(other != null);

    other.forEach((key, value) {
      this[key] = value;
    });
  }

  @override
  void addEntries(Iterable<MapEntry<K, V>> newEntries) {
    for (final entry in newEntries) {
      this[entry.key] = entry.value;
    }
  }

  @override
  bool containsKey(Object key) {
    return _inner.containsKey(key);
  }

  @override
  bool containsValue(Object value) {
    for (var e = head; e != null; e = e.after) {
      if (e.value == value) {
        return true;
      }
    }

    return false;
  }

  @override
  bool get isEmpty => _inner.isEmpty;

  @override
  bool get isNotEmpty => _inner.isNotEmpty;

  @override
  void forEach(void Function(K key, V value) f) {
    for (var e = head; e != null; e = e.after) {
      f(e.key, e.value);
    }
  }

  @override
  void removeWhere(bool Function(K key, V value) predicate) {
    _inner.removeWhere((_key, _value) {
      if (predicate(_key, _value.value)) {
        _afterNodeRemoval(_value);
        return true;
      }
      return false;
    });
  }

  @override
  Iterable<MapEntry<K, V>> get entries {
    final list = <MapEntry<K, V>>[];
    for (var e = head; e != null; e = e.after) {
      list.add(MapEntry(e.key, e.value));
    }
    return list;
  }

  _Entry _createNew(K key, V value) {
    final entry = _Entry(key: key, value: value);
    _linkNodeLast(entry);
    return entry;
  }

  @override
  void operator []=(K key, value) {
    final node = _inner[key];
    if (node == null) {
      _inner[key] = _createNew(key, value);
    } else {
      _afterNodeAccess(node);
    }
  }

  @override
  V putIfAbsent(
    K key,
    V Function() ifAbsent,
  ) {
    assert(ifAbsent != null);

    return _inner.putIfAbsent(key, () {
      final value = ifAbsent();
      return _createNew(key, value);
    })?.value;
  }

  @override
  V update(
    K key,
    V Function(V value) update, {
    V Function() ifAbsent,
  }) {
    assert(update != null);
    _Entry<K, V> updateFunc(_Entry<K, V> _value) {
      final value = update(_value.value);
      _value.value = value;
      _afterNodeAccess(_value);
      return _value;
    }

    if (ifAbsent != null) {
      return _inner.update(key, updateFunc, ifAbsent: () {
        final value = ifAbsent();
        return _createNew(key, value);
      })?.value;
    } else {
      return _inner.update(key, updateFunc)?.value;
    }
  }

  @override
  void updateAll(V Function(K key, V value) update) {
    assert(update != null);

    _inner.updateAll((_key, _value) {
      final value = update(_key, _value.value);
      _value.value = value;

      /// update all values,we need to update all element orders,
      /// witch is not necessary here.

      return _value;
    });
  }

  @override
  Map<RK, RV> cast<RK, RV>() {
    return _inner.cast<RK, RV>();
  }

  @override
  Map<K2, V2> map<K2, V2>(MapEntry<K2, V2> Function(K key, V value) f) {
    throw Exception('Not implement');
  }
}

class _Entry<K, V> {
  final K key;
  V value;
  _Entry<K, V> before;
  _Entry<K, V> after;

  _Entry({
    this.key,
    this.value,
  });
}
