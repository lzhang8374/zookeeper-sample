package com.sample.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class ZkOper {

    private ZooKeeper zooKeeper = null;
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    // 连接
    public void connect(String connectString) {
        try {
            this.zooKeeper = new ZooKeeper(connectString, 2000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    Event.KeeperState state = event.getState();
                    Event.EventType type = event.getType();
                    String path = event.getPath();
                    System.out.println("#####process()####调用####keeperState:" + state + ",eventType:" + type + ",path:" + path);
                    if (Event.KeeperState.SyncConnected == state) {
                        // 连接类型
                        if (Event.EventType.None == type) {
                            countDownLatch.countDown();
                            System.out.println("zk 建立连接");
                        }
                        // 创建类型
                        if (Event.EventType.NodeCreated == type) {
                            System.out.println("####事件通知,当前创建一个新的节点####路径:" + path);
                        }
                        // 修改类型
                        if (Event.EventType.NodeDataChanged == type) {
                            System.out.println("####事件通知,当前创建一个修改节点####路径:" + path);
                        }
                        // 删除类型
                        if (Event.EventType.NodeDeleted == type) {
                            System.out.println("####事件通知,当前创建一个删除节点####路径:" + path);
                        }
                    }
                }
            });
            // 进行阻塞
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 关闭
    public void close() {
        try {
            if (this.zooKeeper != null)
                zooKeeper.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建节点
    public void createNode(String path, String data, boolean isPersistent, boolean isSequential) {
        try {
            Stat stat = this.zooKeeper.exists(path, true);
            if (stat == null) {
                if (isPersistent) {
                    if (isSequential) {
                        this.zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                    } else {
                        this.zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } else {
                    if (isSequential) {
                        this.zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                    } else {
                        this.zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 修改节点
    public void updateNode(String path, String data) {
        try {
            Stat stat = this.zooKeeper.exists(path, true);
            if (stat != null) {
                this.zooKeeper.setData(path, data.getBytes(), -1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 删除节点
    public void delNode(String path) {
        try {
            Stat stat = this.zooKeeper.exists(path, true);
            if (stat != null) {
                this.zooKeeper.delete(path, -1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取节点
    public String getNode(String path) {
        try {
            byte[] data = this.zooKeeper.getData(path, true, new Stat());
            return new String(data);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
