package org.apache.flume.source.taildir.mysql;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by huanghai on 2019/8/19.
 */
public class TailFileCache {
    private static final Logger logger = LoggerFactory.getLogger(TailFileCache.class);

    private static Cache<String,ConcurrentHashMap<String,Object>> cache = null;
    static {
        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(30*60*60, TimeUnit.SECONDS)
//            .expireAfterAccess(10, TimeUnit.SECONDS)
//            .removalListener(new RemovalListener<String, ConcurrentHashMap<String,Object>>(){
//              @Override
//              public void onRemoval(RemovalNotification<String, ConcurrentHashMap<String,Object>> notification) {
//                if (notification.getCause()== RemovalCause.EXPIRED) {
//                  logger.info("cache item expired. key=" + notification.getKey());
//                }
//              }
//            })
                .build();
    }

    public static void removeCache(String key) {
        cache.invalidate(key);
    }

    public static ConcurrentHashMap<String,Object> getOrInitGavaCache(String key) {
        ConcurrentHashMap cacheItem = null;
        try {
            Object item = cache.get(key);
            if (!(item instanceof ConcurrentHashMap)) {
                item = null;
            }
        } catch (ExecutionException e) {
            cacheItem = null;
        } catch (UncheckedExecutionException e) {
            cacheItem = null;
        }
        if (cacheItem==null) {
            cache.put(key, cacheItem = new ConcurrentHashMap<>());
        }
        return cacheItem;
    }

}
