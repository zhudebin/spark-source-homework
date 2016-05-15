package com.zmyuan.sparkSource.hw04;

/**
 * Created by zdb on 2016/5/15.
 */
public class RedisCache implements Cache {
    @Override
    public String get(String key) {
        // select form redis
        return null;
    }

    @Override
    public void saveOrUpdate(String key, String value) {
        // update to redis
    }
}
