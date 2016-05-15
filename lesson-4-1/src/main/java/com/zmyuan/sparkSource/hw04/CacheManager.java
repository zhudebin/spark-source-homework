package com.zmyuan.sparkSource.hw04;

/**
 * Created by zdb on 2016/5/15.
 */
public class CacheManager {

    private MemoryCache firstCache;
    private RedisCache secondCache;

    private CacheManager() {
        firstCache = new MemoryCache();
        secondCache = new RedisCache();
    }

    public static CacheManager getCacheManager() {
        return new CacheManager();
    }


    public String getFromCache(String key) {
        String value = firstCache.get(key);
        if(value != null) {
            return value;
        } else {
            value = secondCache.get(key);
            if(value != null) {
                firstCache.saveOrUpdate(key, value);
            }
        }
        return value;
    }

    public void saveOrUpdateCache(String key, String value) {
        secondCache.saveOrUpdate(key, value);
        firstCache.saveOrUpdate(key, value);
    }
}
