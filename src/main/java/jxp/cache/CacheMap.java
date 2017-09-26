package jxp.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 缓存Map类
 * ==============================
 * 可设置键的有效期，到期后自动清理
 * 线程安全
 */
public class CacheMap {

    private Map<String, Object> map = new HashMap<>();  ///< 缓存
    private ReadWriteLock mapRWL = new ReentrantReadWriteLock();    ///< 缓存读写锁
    private ScheduledExecutorService service = new ScheduledThreadPoolExecutor(2);  ///< 键清理任务
    private int expire = 0; ///< 键有效期(s)，<= 0则永久保存

    /**
     * 添加键清理任务
     * @param key 键
     * @param expire 有效期
     */
    private void addSchedule(String key, int expire) {
        final String finalKey = key;
        service.schedule(new Runnable() {
            @Override
            public void run() {
                mapRWL.writeLock().lock();
                try {
                    map.remove(finalKey);
                } finally {
                    mapRWL.writeLock().unlock();
                }
            }
        }, expire, TimeUnit.SECONDS);
    }

    /**
     * 获取键有效期
     * @return 键有效期(s)
     */
    public int getExpire() {
        return expire;
    }

    /**
     * 设置键有效期
     * @param expire 键有效期(s)
     */
    public void setExpire(int expire) {
        this.expire = expire;
    }

    /**
     * PUT
     * @param key 键
     * @param val 值
     */
    public void put(String key, Object val) {
        put(key, val, expire);
    }

    /**
     * PUT(对键设置特定的有效期)
     * @param key 键
     * @param val 值
     * @param expire 有效期
     */
    public void put(String key, Object val, int expire) {
        mapRWL.writeLock().lock();
        try {
            map.put(key, val);
            if (expire > 0) {
                addSchedule(key, expire);
            }
        } finally {
            mapRWL.writeLock().unlock();
        }
    }

    /**
     * GET
     * @param key 键
     * @return 值
     */
    public Object get(String key) {
        mapRWL.readLock().lock();
        try {
            return map.get(key);
        } finally {
            mapRWL.readLock().unlock();
        }
    }

    /**
     * REMOVE
     * @param key 键
     */
    public void remove(String key) {
        mapRWL.writeLock().lock();
        try {
            map.remove(key);
        } finally {
            mapRWL.writeLock().unlock();
        }
    }

    /**
     * 是否包含指定键
     * @param key 键
     * @return 含有指定键则为true，否则为false
     */
    public boolean containsKey(String key) {
        mapRWL.readLock().lock();
        try {
            return map.containsKey(key);
        } finally {
            mapRWL.readLock().unlock();
        }
    }

    /**
     * 获取缓存快照
     * @return 缓存快照
     */
    public Map<String, Object> snapshot() {
        mapRWL.readLock().lock();
        try {
            return new HashMap<>(map);
        } finally {
            mapRWL.readLock().unlock();
        }
    }
}
