package de.tum.i13.StorageEngine.internal.memory;

import java.util.Arrays;

import de.tum.i13.StorageEngine.internal.Factory;
import de.tum.i13.StorageEngine.internal.Node;
import de.tum.i13.StorageEngine.internal.NonLeaf;
import de.tum.i13.StorageEngine.internal.Options;

public final class NonLeafMemory<K, V> implements NonLeaf<K, V> {

    private final Options<K, V> options;
    private final Factory<K, V> factory;
    private final Node<K, V>[] children;
    private final K[] keys;
    private int numKeys; // number of keys

    @SuppressWarnings("unchecked")
    NonLeafMemory(Options<K, V> options, Factory<K, V> factory) {
        this.options = options;
        this.factory = factory;
        this.children = new Node[options.maxNonLeafKeys() + 1];
        this.keys = (K[]) new Object[options.maxLeafKeys()];
    }

    @Override
    public void setNumKeys(int numKeys) {
        this.numKeys = numKeys;
    }

    @Override
    public int numKeys() {
        return numKeys;
    }

    @Override
    public void setChild(int index, Node<K, V> node) {
        children[index] = node;
    }

    @Override
    public Node<K, V> child(int index) {
        return children[index];
    }

    @Override
    public K key(int index) {
        return keys[index];
    }

    @Override
    public void setKey(int index, K key) {
        keys[index] = key;
    }

    @Override
    public void move(int mid, NonLeaf<K, V> other, int length) {
        other.setNumKeys(length);
        System.arraycopy(this.keys, mid, ((NonLeafMemory<K, V>) other).keys, 0, length);
        System.arraycopy(this.children, mid, ((NonLeafMemory<K, V>) other).children, 0, length + 1);
        numKeys = mid - 1;// this is important, so the middle one elevates to next
        // depth(height), inner node's key don't repeat itself
    }

    @Override
    public void insert(int idx, K key, Node<K, V> node) {
        System.arraycopy(keys, idx, keys, idx + 1, numKeys - idx);
        System.arraycopy(children, idx, children, idx + 1, numKeys - idx + 1);
        children[idx] = node;
        keys[idx] = key;
        numKeys+=1;
    }

    @Override
    public Options<K, V> options() {
        return options;
    }

    @Override
    public Factory<K, V> factory() {
        return factory;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("NonLeafMemory [");
        builder.append("numKeys=" + numKeys());
        builder.append(", keys=");
        builder.append(Arrays.toString(keys));
        builder.append("]");
        return builder.toString();
    }
    
    

}
