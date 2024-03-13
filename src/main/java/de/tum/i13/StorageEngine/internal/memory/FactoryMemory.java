package de.tum.i13.StorageEngine.internal.memory;

import de.tum.i13.StorageEngine.internal.Factory;
import de.tum.i13.StorageEngine.internal.Leaf;
import de.tum.i13.StorageEngine.internal.Node;
import de.tum.i13.StorageEngine.internal.NonLeaf;
import de.tum.i13.StorageEngine.internal.Options;

public final class FactoryMemory<K, V> implements Factory<K, V> {

    private final Options<K, V> options;

    public FactoryMemory(Options<K, V> options) {
        this.options = options;
    }

    @Override
    public Leaf<K, V> createLeaf() {
        return new LeafMemory<K, V>(options, this);
    }

    @Override
    public NonLeaf<K, V> createNonLeaf() {
        return new NonLeafMemory<K, V>(options, this);
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }

    @Override
    public void commit() {
        // do nothing
    }

    @Override
    public void root(Node<K, V> node) {
        // do nothing
    }

    @Override
    public Node<K, V> loadOrCreateRoot() {
        return createLeaf();
    }

    @Override
    public Options<K, V> options() {
        return options;
    }

}
