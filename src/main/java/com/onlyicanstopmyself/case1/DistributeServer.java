package com.onlyicanstopmyself.case1;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {
    private String connectString = "zytCentos:2181,zytCentosClone1:2181,zytCentosClone2:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeServer server = new DistributeServer();
        //1.连接zk
        server.getConnect();
        //2.注册服务器
        server.register(args[0]);
        //3.服务器操作
        server.business(args[0]);
    }

    private void business(String hostName) throws InterruptedException {
        System.out.println(hostName + "is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    private void register(String hostName) throws InterruptedException, KeeperException {
        zk.create("/servers/"+hostName,hostName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostName + " is online");
    }

    private void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
