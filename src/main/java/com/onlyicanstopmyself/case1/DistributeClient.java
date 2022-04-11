package com.onlyicanstopmyself.case1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {

    private ZooKeeper zk;
    private String connectString = "zytCentos:2181,zytCentosClone1:2181,zytCentosClone2:2181";
    private int sessionTimeout = 2000;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeClient client = new DistributeClient();
        //连接
        client.getConnect();
        //进行对服务器下面的子节点的监听
        client.getServerList();
        //执行业务
        client.business();
    }

    private void business() throws InterruptedException {
        System.out.println("正在工作中！");
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getServerList() throws InterruptedException, KeeperException {
        //这里获取的child是每个节点的名字
        List<String> children = zk.getChildren("/servers", true);
        List<String> serversData = new ArrayList<>();

        for (String child : children) {
            byte[] data = zk.getData("/servers/" + child, false, null);
            serversData.add(new String(data));
        }
        System.out.println(serversData);
    }

    private void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServerList();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
