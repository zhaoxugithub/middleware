package com.serendipity.hbase.weibo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase 工具类
 * <p>
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表
 */
public class HBaseUtil {

    public static Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "CentOS201");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return conf;
    }

    /**
     * 创建命名空间
     *
     * @param nameSpace
     * @throws IOException
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        //创建连接
        Connection connection = ConnectionFactory.createConnection(getConfiguration());
        //获取admin对象
        Admin admin = connection.getAdmin();
        //获取descriptor
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(nameSpace).build();
        //创建命名空间
        admin.createNamespace(descriptor);
        //关闭admin
        admin.close();
        //关闭连接
        connection.close();
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     */
    private static boolean isTableExist(String tableName) throws IOException {

        //创建连接
        Connection connection = ConnectionFactory.createConnection(getConfiguration());
        //创建admin
        Admin admin = connection.getAdmin();
        //判断是否存在
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        //关闭admin
        admin.close();
        //关闭连接
        connection.close();
        return result;
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param versions
     * @param colFamily
     */
    public static void  createTable(String tableName, int versions, String... colFamily) throws IOException {
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.printf("表已经存在");
            return;
        }
        //判断列族是否存在
        if (colFamily.length <= 0) {
            System.out.printf("列族不能为空");
            return;
        }
        //创建连接
        Connection connection = ConnectionFactory.createConnection(getConfiguration());
        //创建admin
        Admin admin = connection.getAdmin();
        //表描述器
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //遍历列族
        for (String col : colFamily) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(col));
            //设置版本号
            hColumnDescriptor.setMaxVersions(versions);
            descriptor.addFamily(hColumnDescriptor);
        }
        //创建表
        admin.createTable(descriptor);
        //关闭admin
        admin.close();
        //关闭连接
        connection.close();
    }

    public static void dropTable(String tableName) throws IOException {
        //创建连接
        Connection connection = ConnectionFactory.createConnection(getConfiguration());
        //创建admin
        Admin admin = connection.getAdmin();
        if (isTableExist(tableName)) {
            //删除表之前disable表
            admin.disableTable(TableName.valueOf(tableName));
            //删除表
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表" + tableName + "已经删除");
        } else {
            System.out.println("表" + tableName + "不存在");
        }
    }
}
