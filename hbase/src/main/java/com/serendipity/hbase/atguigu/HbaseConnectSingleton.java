package com.serendipity.hbase.atguigu;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 使用单利饿汉式的方式创建连接对象
 */
public class HbaseConnectSingleton {
    public static Connection connection;
    static {
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }
}
