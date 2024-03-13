package de.tum.i13.StorageEngine.internal;

public interface FactoryProvider<K, V> {

    Factory<K, V> createFactory(Options<K, V> options);

}
