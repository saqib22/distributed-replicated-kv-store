package de.tum.i13.caching;

import java.util.LinkedHashMap;
import java.util.Map;

public class FIFOCache implements Cache{
    LinkedHashMap<String,String> cache;
    int capacity;
    FIFOCache(int capacity) {
        cache = new LinkedHashMap<>(capacity);
        this.capacity = capacity;
    }
    @Override
    public String get(String key) {
        if(!cache.containsKey(key))
            return null;
        return cache.get(key);
    }

    @Override
    public void put(String key, String value) {
        if (cache.containsKey(key) && value.equals("null")){
            cache.remove(key);
            return;
        }

        if (!value.equals("null")) {
            if (cache.containsKey(key)) {
                cache.replace(key, value);
                return;
            } else if (cache.size() == capacity) {
                Map.Entry<String, String> entry = cache.entrySet().iterator().next();
                cache.remove(entry.getKey());
            }
            cache.put(key, value);
        }
    }

    @Override
    public CacheTemp getStatus(String key) {
        if (this.cache.containsKey(key))
            return CacheTemp.CACHE_HIT;
        else
            return CacheTemp.CACHE_MISS;
    }
}
