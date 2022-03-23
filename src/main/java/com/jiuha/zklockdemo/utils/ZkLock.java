package com.jiuha.zklockdemo.utils;

import com.jiuha.zklockdemo.common.Lock;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 根据Zk分布式锁逻辑，实现的分布式锁
 */
@Slf4j
public class ZkLock implements Lock {

    static {
        //todo 读取配置文件属性作为路径
    }

    /**
     * Zookeper节点路径（父节点）
     */
    private static final String ZK_PATH = "/test/lock";
    /**
     * ZK前缀
     */
    public static final String LOCK_PREFIX = ZK_PATH + "/";

    public static final long WAIT_TIME = 1000;

    CuratorFramework client = null;

    /**
     * 排队短号路径
     */
    private String LOCKER_SHORT_PATH = null;
    /**
     * 加锁路径
     */
    private String LOCKED_PATH = null;
    /**
     * 排队等待前一个路径值
     */
    private String PRIOR_PATH = null;
    /**
     * 引用计数，记录重入次数
     */
    final AtomicInteger lockCount = new AtomicInteger(0);
    /**
     * 加锁线程
     */
    private Thread thread;

    public ZkLock() {
        this.client = ZkCuratorUtils.conn();
        synchronized (this.client) {
            try {
                //检查Zookeeper是否有锁节点
                if (!ZkCuratorUtils.checkNodeExist(this.client, ZK_PATH)) {
                    ZkCuratorUtils.createNode(this.client, ZK_PATH, CreateMode.PERSISTENT);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean lock() throws Exception {
        synchronized (this) {
            if (lockCount.get() == 0) {
                thread = Thread.currentThread();
                lockCount.incrementAndGet();
            } else {
                if (!thread.equals(Thread.currentThread())) {
                    return false;
                }
                lockCount.incrementAndGet();
                return true;
            }
        }
        try {
            boolean locked = false;
            //首先尝试着去加锁
            locked = tryLock();
            if (locked) {
                return true;
            }
            //如果加锁失败就去等待
            while (!locked) {
                await();
                //获取等待的子节点列表
                List<String> waiters = getWaiters();
                //判断，是否加锁成功
                if (checkLocked(waiters)) {
                    locked = true;
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            unlock();
        }
        return false;
    }

    /**
     * 进入等待
     */
    private void await() throws Exception {
        //注册监听器监听前一个等待者
        if (null == PRIOR_PATH) {
            throw new Exception("prior_path error");
        }
        final CountDownLatch latch = new CountDownLatch(1);
        //订阅比自己次小顺序节点的删除事件
        Watcher w = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                log.info("监听到的变化 watchedEvent = {}", watchedEvent);
                log.info("[WatchedEvent]节点删除");

                latch.countDown();
            }
        };
        client.getData().usingWatcher(w).forPath(PRIOR_PATH);
        latch.await(WAIT_TIME, TimeUnit.SECONDS);
    }

    /**
     * 检查是否能获取锁
     *
     * @param waiters 等待者列表
     * @return 是否能获取锁
     */
    private boolean checkLocked(List<String> waiters) {
        return waiters.get(0).equals(LOCKED_PATH);
    }

    /**
     * 获取父节点下顺序排序的子节点列表
     *
     * @return 排序过后的子节点列表
     * @throws Exception 异常
     */
    private List<String> getWaiters() throws Exception {
        return ZkCuratorUtils.getChildren(this.client, ZK_PATH)
                .stream().sorted().collect(Collectors.toList());
    }

    @Override
    public boolean unlock() {
        //只有加锁的线程，能够解锁
        if (!thread.equals(Thread.currentThread())) {
            return false;
        }
        //减少可重入的计数
        int newLockCount = lockCount.decrementAndGet();
        //计数不能小于0
        if (newLockCount < 0) {
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + LOCKED_PATH);
        }
        //如果计数不为0，直接返回
        if (newLockCount != 0) {
            return true;
        }
        //删除临时节点
        try {
            if (ZkCuratorUtils.checkNodeExist(this.client, LOCKED_PATH)) {
                ZkCuratorUtils.deleteNode(this.client, LOCKED_PATH);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * 尝试加锁
     *
     * @return 是否加锁成功
     * @throws Exception 异常
     */
    private boolean tryLock() throws Exception {
        //创建临时Znode
        LOCKED_PATH = ZkCuratorUtils.createNode(this.client, LOCK_PREFIX, CreateMode.EPHEMERAL_SEQUENTIAL);
        //然后获取所有节点
        List<String> waiters = getWaiters();
        if (null == LOCKED_PATH) {
            throw new Exception("zk error");
        }
        //取得加锁的排队编号
        LOCKER_SHORT_PATH = getShortPath(LOCKED_PATH);
        //获取等待的子节点列表，判断自己是否第一个
        if (checkLocked(waiters)) {
            return true;
        }
        // 判断自己排第几个
        int index = Collections.binarySearch(waiters, LOCKER_SHORT_PATH);
        if (index < 0) {
            // 网络抖动，获取到的子节点列表里可能已经没有自己了
            throw new Exception("节点没有找到: " + LOCKER_SHORT_PATH);
        }
        //如果自己没有获得锁，则要监听前一个节点
        PRIOR_PATH = ZK_PATH + "/" + waiters.get(index - 1);
        return false;
    }

    private String getShortPath(String lockPath) {
        int index = lockPath.lastIndexOf(ZK_PATH + "/");
        if (index >= 0) {
            index += ZK_PATH.length() + 1;
            return index <= lockPath.length() ? lockPath.substring(index) : "";
        }
        return null;
    }
}
