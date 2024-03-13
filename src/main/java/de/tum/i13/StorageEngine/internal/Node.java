package de.tum.i13.StorageEngine.internal;

import java.util.ArrayList;
import java.util.List;

public interface Node<K, V> {

    // returns null if no split, otherwise returns split info
    Split<K, V> insert(K key, V value);

    K key(int i);

    int numKeys();

    Options<K, V> options();

    Factory<K, V> factory();

    default List<K> keys() {
        List<K> list = new ArrayList<K>();
        for (int i = 0; i < numKeys(); i++) {
            list.add(key(i));
        }
        return list;
    }
    
    /**
     * Returns the position where 'key' should be inserted in a leaf node that has
     * the given keys.
     * 
     * @param key key to insert
     * @return the position where key should be inserted
     */
    int getLocation(K key);

}