package de.tum.i13.StorageEngine;

public final class Entry<K, V> {

    private final K key;
    private final V value;

    private Entry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public static <K, V> Entry<K, V> create(K key, V value) {
        return new Entry<K, V>(key, value);
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

}
