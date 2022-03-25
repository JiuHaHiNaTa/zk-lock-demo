package com.jiuha.zklockdemo;

import com.jiuha.zklockdemo.utils.ZkCuratorUtils;
import com.jiuha.zklockdemo.utils.ZkLock;
import org.apache.curator.framework.CuratorFramework;

public class Test {

    public static void main(String[] args) {
//        CuratorFramework client = ZkCuratorUtils.conn();
//        client.close();
        ZkLock lock = new ZkLock();
        try {
            lock.lock();
            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                System.out.println("完成任务" + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                lock.unlock();
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        lock.client.close();
    }
}
