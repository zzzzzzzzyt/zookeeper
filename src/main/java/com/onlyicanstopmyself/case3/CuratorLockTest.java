package com.onlyicanstopmyself.case3;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorLockTest {
    public static void main(String[] args) {
        //创建分布式锁1
        InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(),"/locks");

        //创建分布式锁2
        InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(),"/locks");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.acquire();
                    System.out.println("线程1获得锁");
                    lock1.acquire();
                    System.out.println("线程1再次获得锁");
                    Thread.sleep(5000);
                    lock1.release();
                    System.out.println("线程1释放锁");
                    lock1.release();
                    System.out.println("线程1再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.acquire();
                    System.out.println("线程2获得锁");
                    lock2.acquire();
                    System.out.println("线程2再次获得锁");
                    Thread.sleep(5000);
                    lock2.release();
                    System.out.println("线程2释放锁");
                    lock2.release();
                    System.out.println("线程2再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    private static CuratorFramework getCuratorFramework() {

        //连接失败策略
        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);

        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString("zytCentos:2181,zytCentosClone1:2181,zytCentosClone2:2181")
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(policy)
                .build();

        //记住启动客户端
        curator.start();

        return curator;
    }
}
