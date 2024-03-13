package de.tum.i13.caching;

public interface Cache {
    public enum CacheTemp{
        CACHE_HIT,
        CACHE_MISS
    }

    /**
     * @return the value that is associated with this message,
     * null if not key is associated.
     */
    public String get(String key);
    public void put(String key, String value);

    public CacheTemp getStatus(String key);
}
