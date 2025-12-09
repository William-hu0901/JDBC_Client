package org.daodao.jdbc.pool;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class DatabaseThreadPoolManager {
    
    private static volatile DatabaseThreadPoolManager instance;
    private final ExecutorService executorService;
    private final ThreadPoolExecutor threadPoolExecutor;
    private static final int CORE_POOL_SIZE = 10;
    private static final int MAX_POOL_SIZE = 100;
    private static final long KEEP_ALIVE_TIME = 60L;
    
    private DatabaseThreadPoolManager() {
        this.threadPoolExecutor = new ThreadPoolExecutor(
            CORE_POOL_SIZE,
            MAX_POOL_SIZE,
            KEEP_ALIVE_TIME,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            r -> {
                Thread thread = new Thread(r, "DB-Worker-" + System.currentTimeMillis());
                thread.setDaemon(false);
                return thread;
            }
        );
        
        this.executorService = Executors.unconfigurableExecutorService(threadPoolExecutor);
        log.info("Database thread pool initialized with core size: {}, max size: {}", CORE_POOL_SIZE, MAX_POOL_SIZE);
    }
    
    public static DatabaseThreadPoolManager getInstance() {
        if (instance == null) {
            synchronized (DatabaseThreadPoolManager.class) {
                if (instance == null) {
                    instance = new DatabaseThreadPoolManager();
                }
            }
        }
        return instance;
    }
    
    public ExecutorService getExecutorService() {
        return executorService;
    }
    
    public ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }
    
    public void shutdown() {
        try {
            log.info("Shutting down database thread pool...");
            executorService.shutdown();
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Thread pool did not terminate gracefully, forcing shutdown");
                executorService.shutdownNow();
            }
            log.info("Database thread pool shutdown completed");
        } catch (InterruptedException e) {
            log.error("Interrupted during thread pool shutdown", e);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    public int getActiveCount() {
        return threadPoolExecutor.getActiveCount();
    }
    
    public int getPoolSize() {
        return threadPoolExecutor.getPoolSize();
    }
    
    public long getCompletedTaskCount() {
        return threadPoolExecutor.getCompletedTaskCount();
    }
}