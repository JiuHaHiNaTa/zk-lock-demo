package com.jiuha.zklockdemo.common;

/**
 * Zk Lock 接口
 *
 * @author Jiuha
 */
public interface Lock {

    /**
     * 加锁方法
     *
     * @return 加锁结果
     * @throws Exception 捕获出现异常
     */
    boolean lock() throws Exception;

    /**
     * 释放锁方法
     *
     * @return 是否成功解锁
     */
    boolean unlock();
}
