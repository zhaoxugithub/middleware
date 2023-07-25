package com.serendipity.zookeeper.dmeo02;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;


/**
 * 原始API
 */
public class ZkClientUtil {

    private static final Logger logger = LoggerFactory.getLogger(ZkClientUtil.class);
    private static ZooKeeper zk;
    private static String zkPath = "1.15.149.196:2181";
    static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    static {
        try {
            zk = new ZooKeeper(zkPath, 1000, new Watcher() {
                //监控所有被触发的条件
                @Override
                public void process(WatchedEvent event) {
                    logger.info("已经触发了{} 事件", event.getType());
                    connectedSemaphore.countDown();
                }
            });
        } catch (Exception e) {
            System.err.println("系统异常");
        }
    }

    public ZooKeeper getZkConnection() {
        try {
            if (zk == null) {
                //如果zk还没有创建，客户端请求连接就会被阻塞
                connectedSemaphore.await();
            }
            return zk;
        } catch (Exception e) {
            System.err.println("zk 初始化失败");
        }
        return null;
    }

    /**
     * 同步创建zk节点
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void create() throws InterruptedException, KeeperException {
        /**
         * "aa3":节点名称
         * test:节点内容
         *ZooDefs.Ids.OPEN_ACL_UNSAFE（使用完全开发的ACL，允许客户端对znode进行读/写）
         *CreateMode.PERSISTENT 使用永久性的属性，当服务端掉线之后，这个节点不会消失
         */
        String aa3 = getZkConnection().create("/aa3", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(aa3);
    }

    /**
     * 异步回调创建zk节点
     */
    public void createASyc() throws InterruptedException, KeeperException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        getZkConnection().create("/aa2", "test2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                System.out.println("rc:" + rc + "&path:" + path + "&ctx:" + ctx + "&name:" + name);
                countDownLatch.countDown();
            }
        }, "异步创建");
        countDownLatch.await();

    }

    /**
     * 同步删除
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void delete() throws InterruptedException, KeeperException {
        // version 表示此次删除针对于的版本号。 传-1 表示不忽略版本号
        getZkConnection().delete("/aa1", -1);
    }

    /**
     * 异步删除
     */
    public void deleteASync() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);
        getZkConnection().delete("/aa1", -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                System.out.println("rc:" + rc + "&path:" + path + "&ctx:" + ctx);
                countDownLatch.countDown();
            }
        }, "异步删除");
        countDownLatch.await();
    }

    /**
     * 同步获取数据，包括子节点和获取和当前节点数据的获取
     */
    public void getChildren() throws InterruptedException, KeeperException {
        Stat stat = new Stat();
        /*
        // path:指定数据节点的节点路径， 即API调用的目的是获取该节点的子节点列表
        // Watcher : 注册的Watcher。一旦在本次获取子节点之后，子节点列表发生变更的话，就会向该Watcher发送通知。Watcher仅会被触发一次。
        // state: 获取指定数据节点(也就是path参数对应的节点)的状态信息(无节点名和数据内容)，传入旧的state将会被来自服务端响应的新state对象替换。
         */
        List<String> children = getZkConnection().getChildren("/", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("我是监听事件，监听子节点变化");
            }
        }, stat);
        System.out.println(stat);
        System.out.println(children);
    }

    /**
     * 异步获取子节点
     *
     * @throws Exception
     */
    public void getChildrenASync() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        getZkConnection().getChildren("/", event -> {
            System.out.println("我是监听事件，监听子节点变化");
        }, (rc, path, ctx, children) -> {
            //异步回调
            System.out.println("children:" + children);
            countDownLatch.countDown();
        }, "context");
        countDownLatch.await();
    }

    /**
     * 同步获取数据
     */
    public void getDataTest() throws InterruptedException, KeeperException {
        Stat stat = new Stat();
        byte[] bytes = getZkConnection().getData("/aa1", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("我是监听事件，监听数据状态发生变化");
            }
        }, stat);
        System.out.println("stat = " + stat);
        System.out.println("bytes = " + new String(bytes));
    }

    /**
     * 异步获取数据
     *
     * @throws InterruptedException
     */
    public void getDataASync() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        getZkConnection().getData("/aa1", event -> {
            System.out.println("我是监听事件，监听数据状态发生变化");
        }, (rc, path, ctx, data, stat) -> {
            System.out.println("获取到的内容是：" + new String(data));
            countDownLatch.countDown();
        }, "121");
        countDownLatch.await();
    }

    /**
     * 同步更新数据
     *
     * @throws Exception
     */
    public void setData() throws Exception {
        byte[] oldValue = getZkConnection().getData("/aa1", false, null);
        System.out.println("更新前值是:" + new String(oldValue));
        Stat stat = getZkConnection().setData("/aa1", "helloWorld".getBytes(), -1);
        byte[] newValue = getZkConnection().getData("/aa1", false, null);
        System.out.println("更新后值是:" + new String(newValue));
    }

    /**
     * 异步更新数据
     *
     * @throws Exception
     */
    public void setDataASync() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        getZkConnection().setData("/aa1", "helloChina".getBytes(), -1, (rc, path, ctx, name) -> {
            System.out.println("更新成功");
            countDownLatch.countDown();
        }, "1111");
        countDownLatch.await();
        byte[] newValue = getZkConnection().getData("/aa1", false, null);
        System.out.println("更新前值是:" + new String(newValue));
    }

    public static void main(String[] args) throws InterruptedException, KeeperException {
        ZkClientUtil zkClientUtil = new ZkClientUtil();
        zkClientUtil.create();
    }
}
