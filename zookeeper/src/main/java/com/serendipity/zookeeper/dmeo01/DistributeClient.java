package com.serendipity.zookeeper.dmeo01;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {

    private static String connections = "192.168.1.201:2181,192.168.1.202:2181,192.168.1.203:2181";

    private static int sessionTimeOut = 2000;

    private ZooKeeper zkClient = null;

    private static String parentNode = "/servers";

    // 创建到zk的客户端连接
    public void connection() throws IOException {

        zkClient = new ZooKeeper(connections, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServersList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // 获取服务器列表信息
    public void getServersList() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(parentNode, true);

        List<String> serverList = new ArrayList<>();
        for (String child : children) {
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            serverList.add(new String(data));
        }
        System.out.println(serverList);
    }

    //处理业务
    public void doBusiness(String hostname) throws InterruptedException {
        System.out.println(hostname + " is working ...");

        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeClient client = new DistributeClient();
        client.connection();
        client.getServersList();
        client.doBusiness("client");
    }
}
