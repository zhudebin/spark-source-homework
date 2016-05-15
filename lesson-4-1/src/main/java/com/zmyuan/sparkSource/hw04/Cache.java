package com.zmyuan.sparkSource.hw04;

/**
 * Created by zdb on 2016/5/15.
 */
public interface Cache {

    String get(String key);

    void saveOrUpdate(String key, String value);

}
