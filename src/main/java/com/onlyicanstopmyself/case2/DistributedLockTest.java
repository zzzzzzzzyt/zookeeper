package com.onlyicanstopmyself.case2;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class DistributedLockTest {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        final DistributedLock lock1 = new DistributedLock();
        final DistributedLock lock2 = new DistributedLock();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.zkLock();
                    System.out.println("线程1获取到锁");
                    Thread.sleep(5*1000);
                    lock1.unZkLock();
                    System.out.println("线程1释放锁");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.zkLock();
                    System.out.println("线程2获取到锁");
                    Thread.sleep(5*1000);
                    lock2.unZkLock();
                    System.out.println("线程2释放锁");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
