package com.serendipity.zookeeper.dmeo01;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {

    private static String connections = "192.168.1.201:2181,192.168.1.202:2181,192.168.1.203:2181";
    private static int sessionTimeOut = 2000;
    private ZooKeeper zkClient = null;
    private static String parentNode = "/servers";

    //创建连接
    public void connection() throws IOException {
        zkClient = new ZooKeeper(connections, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    //注册服务器
    public void registerServer(String hostname) throws KeeperException, InterruptedException {
        String create = zkClient.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online " + create);
    }

    //处理业务
    public void doBusiness(String hostname) throws InterruptedException {
        System.out.println(hostname + " is working ...");

        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer distributeServer = new DistributeServer();
        distributeServer.connection();
        distributeServer.registerServer(args[0]);
        distributeServer.doBusiness(args[0]);
    }
}
