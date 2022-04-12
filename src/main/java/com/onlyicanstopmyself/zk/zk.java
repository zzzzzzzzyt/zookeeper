package com.onlyicanstopmyself.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class zk {
    private ZooKeeper zkClient;
    //千万注意创建连接服务端 前后不能随便加空格
    private String connectString = "zytCentos:2181,zytCentosClone1:2181,zytCentosClone2:2181";
    private int sessionTimeout = 2000;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
//                List<String> children = null;
//                try {
//                    children = zkClient.getChildren("/", true);
//                    for (String child : children) {
//                        System.out.println(child);
//                    }
//                } catch (KeeperException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        });
    }

    @Test
    public void create() throws InterruptedException, KeeperException {
        zkClient.create("/zyt","被美团录取啦".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        zkClient.getChildren("/", true);
        Thread.sleep(Long.MAX_VALUE);
    }


    @Test
    public void isExist() throws InterruptedException, KeeperException {
        Stat exists = zkClient.exists("/sasa", false);
        System.out.println(exists==null?"no exist":"exist");
    }
}
