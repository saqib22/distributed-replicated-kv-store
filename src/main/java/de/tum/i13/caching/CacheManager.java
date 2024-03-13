package de.tum.i13.caching;

public class CacheManager {
    private final String strategy;
    private final int cache_capacity;

    public CacheManager(String strategy, int cache_capacity){
        this.strategy = strategy;
        this.cache_capacity = cache_capacity;
    }

    public Cache getCache(){
        switch (this.strategy){
            case "LRU": return new LRUCache(this.cache_capacity);
            case "FIFO": return new FIFOCache(this.cache_capacity);
            case "LFU": return new LFUCache(this.cache_capacity);
            default: return null;
        }

    }
}
