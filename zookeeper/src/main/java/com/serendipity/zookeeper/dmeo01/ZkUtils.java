package com.serendipity.zookeeper.dmeo01;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZkUtils {

    private static String connections = "10.20.121.4:2181";
    private static int sessionTimeOut = 2000;
    private ZooKeeper zkClient = null;

    /**
     * 初始化连接
     *
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connections, sessionTimeOut, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());
                try {
                    List<String> children = zkClient.getChildren("/", true);
                    children.forEach((x) -> {
                        System.out.println(x);
                    });
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 创建永久性节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        String s = zkClient.create("/oo", "zhaoxu".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(s);
    }

    @Test
    public void list() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", null);
        children.forEach(System.out::println);
    }

    /**
     * 获取子节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String str : children) {
            System.out.println(str);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 判断节点是否存在
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void isExist() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/atguigu", false);
        System.out.println(exists == null ? "not exist" : "exist");
    }
}
