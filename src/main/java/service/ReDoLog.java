package service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReDoLog {
    NormalStore normalStore;
    public ReDoLog(NormalStore normalStore){
        this.normalStore = normalStore;
    }
    // 定义一个线程池，大小根据实际需求调整
    private ExecutorService executor = Executors.newFixedThreadPool(2); // 例如这里设为2，可以同时处理两个任务

    // 回放功能
    public void ReDoLog() {
        // 提交任务到线程池执行
        executor.submit(normalStore::reloadIndex);
        executor.submit(normalStore::zipFile);

        // 关闭线程池
        executor.shutdown();
    }

    // 实际的业务逻辑方法
    private void reloadIndex() {
        // 实现 reloadIndex 的逻辑
        System.out.println("Reloading index...");
    }
}
