package com.jiuha.zklockdemo.utils;

import com.jiuha.zklockdemo.common.CustomAction;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 使用Curator连接Zk服务
 *
 * @author Jiuha
 */
@Slf4j
public class ZkCuratorUtils {

    /**
     * 监听器逻辑线程
     */
    //todo 编码规范不允许Executors创建线程池
    private static ExecutorService pool = Executors.newFixedThreadPool(2);

    public static final String ZK_ADDRESS = "192.168.12.148:2181";

    public static CuratorFramework conn() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(ZK_ADDRESS)
                .connectionTimeoutMs(5000)
                //RetryOneTime RetryNTimes(times,second)
                .retryPolicy(new RetryNTimes(3, 3000))
                .namespace("curator")
                .build();
        client.start();
        log.info("Zk客户端已经连接");
        return client;
    }

    public static boolean checkNodeExist(CuratorFramework client, String path) throws Exception {
        return Objects.nonNull(client.checkExists().forPath(path));
    }

    /**
     * 创建节点
     *
     * @param client Zk客户端
     * @param path   节点路径
     * @throws Exception 异常
     */
    public static String createNode(CuratorFramework client, String path, CreateMode createMode) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        log.info("{}节点是否存在：{}", path, stat.toString());
        return client.create()
                .withMode(Objects.nonNull(createMode) ? createMode : CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(path, path.getBytes());
    }

    /**
     * 创建节点,若父节点不存在
     *
     * @param client Zk客户端
     * @param path   节点路径
     * @throws Exception 异常
     */
    public static String createNodeWithoutParent(CuratorFramework client, String path, CreateMode createMode) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        log.info("{}节点是否存在：{}", path, stat);
        return client.create()
                .creatingParentsIfNeeded()
                .withMode(Objects.nonNull(createMode) ? createMode : CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(path, path.getBytes());
    }

    /**
     * 创建节点带访问控制
     *
     * @param client Zk客户端
     * @param path   节点路径
     * @throws Exception 异常
     */
    public static void createNode(CuratorFramework client, String path, CreateMode createMode, List<ACL> aclList) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        log.info("{}节点是否存在：{}", path, stat);
        client.create()
                .withMode(Objects.nonNull(createMode) ? createMode : CreateMode.EPHEMERAL_SEQUENTIAL)
                .withACL(aclList)
                .forPath(path, path.getBytes());
    }

    /**
     * 更新节点
     *
     * @param client  Zk客户端
     * @param path    节点路径
     * @param version 版本
     * @return 是否成功
     * @throws Exception 异常
     */
    public static boolean updateNode(CuratorFramework client, String path, String message, int version) throws Exception {
        Stat stat = client.setData()
                .withVersion(version)
                .forPath(path, message.getBytes());
        return Objects.nonNull(stat);
    }

    /**
     * 获取某个节点的所有子节点
     *
     * @param client Zk客户端
     * @param path   路径
     * @return 子节点列表（是否排序）
     * @throws Exception 异常
     */
    public static List<String> getChildren(CuratorFramework client, String path) throws Exception {
        return client.getChildren().forPath(path);
    }

    /**
     * 级联删除节点以及其子节点，若删除失败反复重试
     *
     * @param client Zk连接客户端
     * @param path   路径
     * @throws Exception 其他异常
     */
    public static void deleteNode(CuratorFramework client, String path) throws Exception {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
    }

    /**
     * 监听节点的变化
     *
     * @param client Zk客户端
     * @param path   节点路径
     * @throws Exception 异常
     */
    public static void watch(CuratorFramework client, String path) throws Exception {
        NodeCache nodeCache = new NodeCache(client, path, false);
        nodeCache.start(true);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                log.info("节点信息发生变化");
            }
        }, pool);
    }

    /**
     * 排斥锁
     *
     * @param client   Zk客户端
     * @param lockPath 锁路径
     * @param action   实际业务逻辑
     */
    public static void doWithLock(CuratorFramework client, String lockPath, CustomAction action) {
        InterProcessMutex lock = new InterProcessMutex(client, lockPath);
        try {
            if (lock.acquire(10 * 1000, TimeUnit.SECONDS)) {
                //doSomething
                action.process();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
