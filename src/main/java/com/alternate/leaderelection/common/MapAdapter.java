package com.alternate.leaderelection.common;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public abstract class MapAdapter<K, V> implements Map<K, V> {
    @Override
    public int size() {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public V get(Object key) {
        return null;
    }

    @Override
    public V put(K key, V value) {
        return null;
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("method not supported");
    }
}
