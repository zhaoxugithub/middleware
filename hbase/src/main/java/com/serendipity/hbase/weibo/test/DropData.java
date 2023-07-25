package com.serendipity.hbase.weibo.test;

import com.serendipity.hbase.util.HBaseConfig;
import com.serendipity.hbase.weibo.constants.Constant;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DropData {

    public static void main(String[] args) throws IOException, InterruptedException {

        //删除数据
        deleteData(Constant.CONTENT_TABLE);
        deleteData(Constant.RELATION_TABLE);
        deleteData(Constant.INBOX_TABLE);
        Thread.sleep(1000);
        //删除表
        dropTable(Constant.CONTENT_TABLE);
        dropTable(Constant.RELATION_TABLE);
        dropTable(Constant.INBOX_TABLE);
        Thread.sleep(1000);
        //删除命名空间
        dropNameSpace(Constant.NameSpace);
    }

    public static void dropNameSpace(String nameSpace) throws IOException {

        Connection connection = ConnectionFactory.createConnection(HBaseConfig.getConfiguration());
        Admin admin = connection.getAdmin();
        admin.deleteNamespace(nameSpace);
        admin.close();
        connection.close();
    }

    public static void dropTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(HBaseConfig.getConfiguration());
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        admin.close();
        connection.close();
    }

    public static void deleteData(String tableName) throws IOException {

        Connection connection = ConnectionFactory.createConnection(HBaseConfig.getConfiguration());
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        List<byte[]> rowKeyList = new ArrayList<>();
        for (Result result : scanner) {
            rowKeyList.add(result.getRow());
        }
        deleteData(table, rowKeyList);
        connection.close();
    }

    public static void deleteData(Table table, List<byte[]> rowKeyList) throws IOException {
        List<Delete> toDeleteList = new ArrayList<>();
        for (byte[] by : rowKeyList) {
            Delete delete = new Delete(by);
            toDeleteList.add(delete);
        }
        table.delete(toDeleteList);
        table.close();
    }

}
