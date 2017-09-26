package jxp.watcher;

import jxp.cache.CacheMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 文件监视类
 * ==============================
 * 1.使用JDK1.7后新增的文件消息监控类，节省资源
 * 2.监控目录下有文件夹创建时，对新增的文件夹也进行监控
 * 3.文件更新消息集中处理。在设定的周期内收集、合并文件变更消息，一个周期结束后批量处理，
 *   并添加mtime验证，避免了文件更新消息的重复处理
 *
 * 使用方法：
 *   重写doFileModified()
 * 注意：
 *   1.文件创建时也会产生文件变更的消息，因此doFileModified()对新创建的文件也有效
 *   2.文件只修改文件名称时不产生文件修改消息，如有需求需要在文件创建消息下添加具体处理
 */
public class FileWatcher {

    private int USED_KEY_EXPIRE = 300;  ///< 已处理消息列表键有效期

    private Map<WatchKey, String> watchedPathMap = new ConcurrentHashMap<>(); ///< 监视路径集合

    private Thread watchThread;         ///< 监视线程
    private Thread procThread;          ///< 监视消息处理线程

    private CacheMap msgCacheNew;       ///< 未处理过的消息列表缓存
    private CacheMap msgCacheUsed;      ///< 已处理过的消息列表缓存

    private Lock msgCacheLock = new ReentrantLock();   ///< 消息列表缓存锁

    private int procIntervalSec = 1;    ///< 文件消息处理时间间隔

    private WatchService watchService;  ///< 文件监视服务

    public int getProcIntervalSec() {
        return procIntervalSec;
    }

    public void setProcIntervalSec(int procIntervalSec) {
        this.procIntervalSec = procIntervalSec;
    }

    /**
     * 初始化
     */
    private void init() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            e.printStackTrace();
        }
        msgCacheUsed = new CacheMap();
        msgCacheNew = new CacheMap();
    }

    /**
     * 创建消息列表缓存和文件监视服务实例
     */
    public FileWatcher() {
        init();
    }

    /**
     * 注册监视路径
     * @param path 待监视路径
     * @return 注册成功返回true，否则为false
     */
    public boolean pathRegister(String path) {
        File file = FileUtils.getFile(path);
        //获取路径下所有的子目录
        Collection<File> fileList =
                FileUtils.listFilesAndDirs(file, DirectoryFileFilter.INSTANCE, DirectoryFileFilter.INSTANCE);

        try {
            for (File dir : fileList) {
                //注册路径
                WatchKey key = dir.toPath().register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_DELETE);

                //保存路径，在处理消息时能够获取文件的完整路径
                watchedPathMap.put(key, dir.getAbsolutePath());
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 启动
     */
    public void start() {

        //创建监视线程
        watchThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!watchThread.isInterrupted()) {
                    WatchKey key = null;
                    try {
                        key = watchService.take();
                    } catch (InterruptedException e) {
                        break;
                    }

                    for (WatchEvent evt : key.pollEvents()) {
                        //过滤失败消息
                        if (StandardWatchEventKinds.OVERFLOW == evt.kind()) {
                            continue;
                        }

                        //触发消息文件的绝对路径
                        String absolutePath = watchedPathMap.get(key) + File.separator + evt.context();
                        File file = FileUtils.getFile(absolutePath);

                        //创建消息
                        if (StandardWatchEventKinds.ENTRY_CREATE == evt.kind()) {
                            //如果创建的是目录，则对目录进行注册
                            if (file.isDirectory()) {
                                pathRegister(absolutePath);
                            }
                        }

                        //删除消息
                        if (StandardWatchEventKinds.ENTRY_DELETE == evt.kind()) {
                            //确保移除未处理或已处理的消息
                            msgCacheLock.lock();
                            try {
                                msgCacheNew.remove(absolutePath);
                                msgCacheUsed.remove(absolutePath);
                            } finally {
                                msgCacheLock.unlock();
                            }
                        }

                        //更新消息
                        if (StandardWatchEventKinds.ENTRY_MODIFY == evt.kind()) {
                            //只响应文件更新消息
                            if (file.isFile()) {
                                msgCacheLock.lock();
                                try {
                                    long mtime = file.lastModified();

                                    Long lastMtime = (Long) msgCacheNew.get(absolutePath);
                                    if (lastMtime == null)
                                        lastMtime = (Long) msgCacheUsed.get(absolutePath);

                                    if (!Long.valueOf(mtime).equals(lastMtime)) {
                                        //确保移除已处理的消息
                                        msgCacheUsed.remove(absolutePath);
                                        //添加新的待处理消息
                                        msgCacheNew.put(absolutePath, mtime);
                                    }
                                } finally {
                                    msgCacheLock.unlock();
                                }
                            }
                        }

                        if (!key.reset()) {
                            break;
                        }
                    }

                }
            }
        });

        //创建消息处理线程
        procThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!procThread.isInterrupted()) {
                    msgCacheLock.lock();
                    try {
                        Map<String, Object> snapshotNew = msgCacheNew.snapshot();
                        //获取所有更新过的文件路径
                        for (String keyNew : snapshotNew.keySet()) {
                            Long mtime = (Long) msgCacheNew.get(keyNew);
                            //do
                            doFileModified(keyNew, mtime);

                            //移除已处理过的消息
                            msgCacheUsed.put(keyNew, mtime, USED_KEY_EXPIRE);
                            msgCacheNew.remove(keyNew);
                        }
                    } finally {
                        msgCacheLock.unlock();
                    }

                    //处理消息间隔时间
                    try {
                        Thread.sleep(procIntervalSec * 1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }
        });


        //启动监视线程
        watchThread.start();
        //启动消息处理线程
        procThread.start();
    }

    /**
     * 关闭
     */
    public void close() {
        //停止监视线程
        if (procThread != null)
            procThread.interrupt();
        //停止消息处理线程
        if (watchThread != null)
            watchThread.interrupt();
        //关闭监视服务
        try {
            watchService.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        init();
    }

    /**
     * 文件更新时的操作
     * @param filePath 文件绝对路径
     * @param mtime 文件更新时间
     */
    public void doFileModified(String filePath, long mtime) {
        String format = DateFormatUtils.format(mtime, "yyyy-MM-dd hh:mm:ss");
        System.out.println("文件已更新 path:" + filePath + " mtime:" + format);
    }
}
