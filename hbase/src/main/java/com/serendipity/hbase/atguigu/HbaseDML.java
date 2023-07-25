package com.serendipity.hbase.atguigu;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDML {
    public static Connection connection = HbaseConnectSingleton.connection;

    /**
     * 插入数据
     *
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param value
     */
    public static void putCell(String nameSpace, String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {
        // 获取Table
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        // 调用相关方法
        // 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 给put对象添加列族，列，值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
        try {
            // 将对象写入对应的方法
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    /**
     * 读取数据
     *
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     */
    public static void getCells(String nameSpace, String tableName, String rowKey,
                                String columnFamily, String columnName) throws IOException {
        // 获取table
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
    }

    /**
     * 扫描数据
     *
     * @param nameSpace
     * @param tableName
     * @param rowStart
     * @param rowEnd
     */
    public static void scanRows(String nameSpace, String tableName, String rowStart, String rowEnd) {
    }

    /**
     * 带过滤扫描数据
     *
     * @param nameSpace
     * @param tableName
     * @param rowStart
     * @param rowEnd
     * @param columnFamily
     * @param columnName
     * @param value
     */
    public static void filterScan(String nameSpace, String tableName, String rowStart, String rowEnd,
                                  String columnFamily, String columnName, String value) {
    }

    /**
     * 删除数据
     *
     * @param nameSpace
     * @param tableName
     * @param columnFamily
     * @param columnName
     * @param rowKey
     */
    public static void deleteColumn(String nameSpace, String tableName, String columnFamily,
                                    String columnName, String rowKey) {
    }

    public static void main(String[] args) throws IOException {
        putCell("atguigu", "student", "1001", "info", "name", "zx");
        HbaseConnectSingleton.closeConnection();
    }
}
