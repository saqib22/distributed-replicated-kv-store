package de.tum.i13.caching;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.Cache;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

public class LFUCache implements de.tum.i13.caching.Cache{
    //Create a singleton CacheManager using defaults
    CacheManager manager = CacheManager.create();
    Cache testCache;

    public LFUCache(int cache_capacity){
        //Create a Cache specifying its configuration.
        testCache = new net.sf.ehcache.Cache(
                new CacheConfiguration("testCache", cache_capacity)
                        .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LFU)
                        .eternal(false)
                        .timeToLiveSeconds(60)
                        .timeToIdleSeconds(30)
                        .diskExpiryThreadIntervalSeconds(0)
                        /*.persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.LOCALTEMPSWAP))*/);
        manager.addCacheIfAbsent(testCache);
    }
    @Override
    public String get(String key) {
        return testCache.get(key).getObjectValue().toString();
    }

    @Override
    public void put(String key, String value) {
        if (value.equalsIgnoreCase("null")){
            testCache.remove(key);
        }
        else {
            testCache.put(new Element(key, value));
        }
    }

    @Override
    public CacheTemp getStatus(String key) {
        if (testCache.isKeyInCache(key)){
            return CacheTemp.CACHE_HIT;
        }
        else {
            return CacheTemp.CACHE_MISS;
        }
    }
}
