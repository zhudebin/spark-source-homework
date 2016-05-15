package com.zmyuan.sparkSource.hw04;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zdb on 2016/5/15.
 */
public class MemoryCache implements Cache {

    private ConcurrentHashMap<String, CacheData> data = new ConcurrentHashMap();

    public MemoryCache() {
        new Thread() {
            @Override
            public void run() {
                while(true) {
                    MemoryCache.this.cleanExpiredData();
                    System.out.println("------- clean cache");
                    try {
                        sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }

    @Override
    public String get(String key) {
        return data.get(key).data;
    }

    @Override
    public void saveOrUpdate(String key, String value) {

        CacheData cacheData = data.get(key);

        if(cacheData != null) {
            cacheData.updateData(value);
        } else {
            if(data.size() >= 10000) {
                // do nothing  or fifo to manager cache data
            } else {
                data.put(key, new CacheData(value));
            }
        }
    }

    private void cleanExpiredData() {
        long now = new Date().getTime();
        List<String> keys = new ArrayList<>(data.size());
        for(Map.Entry<String, CacheData> en : data.entrySet()) {
            if(en.getValue().isExpired(now)) {
                keys.add(en.getKey());
            }
        }
        synchronized (data) {
            for(String key : keys) {
                data.remove(key);
            }
        }
    }

    private static class CacheData {

        String data;
        long lastTime;

        public CacheData(String data) {
            this.data = data;
            this.lastTime = new Date().getTime();
        }

        public boolean isExpired(long now) {
            if(now - lastTime > 10000) {
                return true;
            }
            return false;
        }

        public void updateData(String data) {
            this.data = data;
            this.lastTime = new Date().getTime();
        }
    }
}
