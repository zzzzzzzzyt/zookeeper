package com.onlyicanstopmyself.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {

    private final ZooKeeper zk;
    private final String connectString = "zytCentos:2181,zytCentosClone1:2181,zytCentosClone2:2181";
    private final int sessionTimeout = 2000;

    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);
    private String waitPath;
    private String currentMode;

    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        //先进行连接
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //countDownLatch 检测连接是否创建完毕 进行等待减少
                if (watchedEvent.getState()==Event.KeeperState.SyncConnected)
                {
                    countDownLatch.countDown();
                }
                //waitLatch 检测是否下线 通知
                if (watchedEvent.getType()==Event.EventType.NodeDeleted&&watchedEvent.getPath().equals(waitPath))
                {
                    waitLatch.countDown();
                }

            }
        });
        countDownLatch.await();
        //判断是否存在根节点
        Stat stat = zk.exists("/locks", false);
        if (stat==null)
        {
            //创建一个根节点
            zk.create("/locks","locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
    }

    //加锁
    public void zkLock()
    {
        //创建临时带序号节点
        try {
            currentMode = zk.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            //判断节点是否是最小节点，如果是获取到锁，如果不是的话对前面的锁进行监听
            List<String> children = zk.getChildren("/locks", false);
            //如果children只有一个值那么直接获取锁就行了
            if (children.size()==1)return;
            else
            {
                Collections.sort(children);//判断是不是第一个就行
                String thisNode = currentMode.substring("/locks/".length());
                //查询当前节点在整个集合中的位置
                int index = children.indexOf(thisNode);
                if (index==0)
                {
                    //说明当前位置在第一个，可以直接获取锁了
                    return;
                }
                else if (index==-1)
                {
                    System.out.println("出现异常或者服务已经下线");
                }
                else
                {
                    //需要监听他前一个节点的
                    waitPath = "/locks/"+children.get(index-1);
                    zk.getData(waitPath,true,null);

                    //等待监听
                    waitLatch.await();
                    //获取锁返回
                    return;
                }
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //解锁
    public void unZkLock()
    {
        //删除节点
        try {
            zk.delete(currentMode,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
