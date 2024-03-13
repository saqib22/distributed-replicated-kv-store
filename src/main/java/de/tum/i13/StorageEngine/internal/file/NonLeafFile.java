package de.tum.i13.StorageEngine.internal.file;

import de.tum.i13.StorageEngine.internal.Factory;
import de.tum.i13.StorageEngine.internal.Node;
import de.tum.i13.StorageEngine.internal.NonLeaf;
import de.tum.i13.StorageEngine.internal.Options;

public final class NonLeafFile<K, V> implements NonLeaf<K, V>, NodeFile {

    private final FactoryFile<K, V> factory;
    private long position;

    public NonLeafFile(FactoryFile<K, V> factory, long position) {
        this.factory = factory;
        this.position = position;
    }

    @Override
    public Options<K, V> options() {
        return factory.options();
    }

    @Override
    public Factory<K, V> factory() {
        return factory;
    }

    @Override
    public void setNumKeys(int numKeys) {
        factory.nonLeafSetNumKeys(position, numKeys);
    }

    @Override
    public int numKeys() {
        return factory.nonLeafNumKeys(position);
    }

    @Override
    public void setChild(int index, Node<K, V> node) {
        factory.nonLeafSetChild(position, index, (NodeFile) node);
    }

    @Override
    public Node<K, V> child(int index) {
        return factory.nonLeafChild(position, index);
    }

    @Override
    public K key(int index) {
        return factory.nonLeafKey(position, index);
    }

    @Override
    public void setKey(int index, K key) {
        factory.nonLeafSetKey(position, index, key);
    }

    @Override
    public void move(int mid, NonLeaf<K, V> other, int length) {
        factory.nonLeafMove(position, mid, length, (NonLeafFile<K, V>) other);

    }

    @Override
    public void insert(int idx, K key, Node<K, V> left) {
        factory.nonLeafInsert(position, idx, key, (NodeFile) left);
    }

    @Override
    public long position() {
        return position;
    }
    
    @Override
    public void position(long position) {
        this.position = position;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("NonLeafFile [");
        b.append("position=");
        b.append(position);
        b.append(", numKeys=");
        b.append(numKeys());
        b.append(", keys=[");
        StringBuilder b2 = new StringBuilder();
        int n = numKeys();
        for (int i = 0; i < n; i++) {
            if (b2.length() > 0) {
                b2.append(", ");
            }
            b2.append(key(i));
        }
        b.append(b2.toString());
        b.append("]");
        b.append("]");
        return b.toString();
    }

}
