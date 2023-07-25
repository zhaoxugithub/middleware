package com.serendipity.hbase.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class HBaseConnect {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "1.15.149.196:12181");

        // 创建hbase同步连接（默认）
        Connection connection = ConnectionFactory.createConnection(conf);
        // 可以使用异步李建
        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(conf);

        System.out.println("asyncConnection = " + asyncConnection);
        System.out.println("connection = " + connection);
        connection.close();

    }
}
