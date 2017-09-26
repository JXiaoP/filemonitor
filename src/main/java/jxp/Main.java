package jxp;

import jxp.watcher.FileWatcher;

public class Main {
    /**
     * 调用样例
     * @param args
     */
    public static void main(String[] args) {
        FileWatcher watcher = new FileWatcher();
        watcher.pathRegister("f:\\123");    //参数为需要监控的目录
        watcher.start();
    }
}
