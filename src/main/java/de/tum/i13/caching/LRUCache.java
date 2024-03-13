package de.tum.i13.caching;

import java.util.LinkedHashMap;
import java.util.Map;

class LRUCache implements Cache{
    private LinkedHashMap<String, String> map;
    private final int CAPACITY;
    public LRUCache(int capacity)
    {
        CAPACITY = capacity;
        map = new LinkedHashMap<String, String>(capacity, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry eldest)
            {
                return size() > CAPACITY;
            }
        };
    }

    @Override
    public String get(String key)
    {
        return map.get(key);
    }

    @Override
    public void put(String key, String value)
    {
        if (map.containsKey(key) && value.equals("null")){
            map.remove(key);
        }
        else if (!value.equals("null"))
            map.put(key, value);
    }

    @Override
    public CacheTemp getStatus(String key){
        if (map.containsKey(key)) return CacheTemp.CACHE_HIT;
        else return CacheTemp.CACHE_MISS;
    }

}