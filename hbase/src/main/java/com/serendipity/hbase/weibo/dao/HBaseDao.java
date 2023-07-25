package com.serendipity.hbase.weibo.dao;

import com.serendipity.hbase.weibo.constants.Constant;
import com.serendipity.hbase.weibo.utils.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 需求：
 * 1) 创建命名空间以及表名的定义
 * 2) 创建微博内容表
 * 3) 创建用户关系表
 * 4) 创建用户微博内容接收邮件表
 * <p>
 * <p>
 * 5) 发布微博内容
 * 6) 添加关注用户
 * 7) 移除（取关）用户
 * 8) 获取关注的人的微博内容
 * 9) 测试
 */
public class HBaseDao {

    /**
     * 发布微博
     *
     * @param uid
     * @param content
     */
    public static void publishWeiBo(String uid, String content) throws IOException {
        //获取客户端连接
        Connection connection = ConnectionFactory.createConnection(HBaseUtil.getConfiguration());
        /**
         * 第一步发布微博信息
         */
        //获取微博表
        Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));
        //组装微博表rowKey

        Long ts = System.currentTimeMillis();
        String rowKey = uid + "_" + ts;
        //组装put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(Constant.CONTENT_TABLE_CF), Bytes.toBytes("content"), ts, Bytes.toBytes(content));
        //添加put
        contentTable.put(put);
        /**
         * 第二步获取 用户粉丝
         */
        //获取用户表
        Table relationTable = connection.getTable(TableName.valueOf(Constant.RELATION_TABLE));

        //封装查询get
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constant.RELATION_TABLE_CF2));
        //获取查询结果集合
        Result result = relationTable.get(get);
        List<Put> inboxList = new ArrayList<>();
        for (Cell cell : result.rawCells()) {
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constant.INBOX_TABLE_CF1), Bytes.toBytes(uid), ts, Bytes.toBytes(rowKey));
            inboxList.add(inboxPut);
        }
        //如果粉丝不为0
        if (inboxList.size() > 0) {
            /**
             * 第三步在微博收件箱表里添加数据
             */
            Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX_TABLE));
            //批量添加微博收件箱数据
            inboxTable.put(inboxList);
            //关闭资源
            inboxTable.close();
        }
        //关闭资源
        relationTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * 关注用户
     *
     * @param uid
     * @param attends
     */
    public static void addAttends(String uid, String... attends) throws IOException {

        if (uid == null || uid.length() <= 0 || attends == null || attends.length <= 0) {
            return;
        }

        Connection connection = ConnectionFactory.createConnection(HBaseUtil.getConfiguration());
        Table relationTable = connection.getTable(TableName.valueOf(Constant.RELATION_TABLE));
        //在用户关系表中添加attends
        List<Put> attendList = new ArrayList<>();
        for (String attend : attends) {
            Put put = new Put(Bytes.toBytes(uid));
            put.addColumn(Bytes.toBytes(Constant.RELATION_TABLE_CF1), Bytes.toBytes(attend), null);
            attendList.add(put);
        }
        relationTable.put(attendList);

        //给关注者添加粉丝
        List<Put> fansList = new ArrayList<>();
        for (String attend : attends) {
            Put put = new Put(Bytes.toBytes(attend));
            put.addColumn(Bytes.toBytes(Constant.RELATION_TABLE_CF2), Bytes.toBytes(uid), null);
            fansList.add(put);
        }
        relationTable.put(fansList);

        Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));

        List<byte[]> rowKeyList = new ArrayList<>();

        for (String attend : attends) {
            Scan scan = new Scan(Bytes.toBytes(attend), Bytes.toBytes("_"));
            ResultScanner scanner = contentTable.getScanner(scan);
            for (Result result : scanner) {
                //获取被关注用户的信息rowKey
                rowKeyList.add(result.getRow());
            }
        }

        if (rowKeyList.size() <= 0) {
            return;
        }
        //在微博收件箱表里添加数据
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX_TABLE));
        List<Put> inboxList = new ArrayList<>();

        for (byte[] rowKey : rowKeyList) {
            String rowK = Bytes.toString(rowKey);
            String id = rowK.substring(0, rowK.indexOf("_"));
            Long ts = Long.parseLong(rowK.substring(rowK.indexOf("_") + 1));
            Put put = new Put(Bytes.toBytes(uid));
            put.addColumn(Bytes.toBytes(Constant.INBOX_TABLE_CF1), Bytes.toBytes(id), ts, rowKey);
            inboxList.add(put);
        }
        inboxTable.put(inboxList);

        inboxTable.close();
        relationTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * 取消关注用户
     *
     * @param uid
     * @param dels
     */
    public static void deleteAttends(String uid, String... dels) throws IOException {

        if (uid == null || uid.length() <= 0 || dels == null || dels.length <= 0) {
            return;
        }
        //创建连接
        Connection connection = ConnectionFactory.createConnection(HBaseUtil.getConfiguration());
        //获取用户关系表
        Table relationTable = connection.getTable(TableName.valueOf(Constant.RELATION_TABLE));

        List<Delete> toDeletePut = new ArrayList<>();
        // 被关注的用户
        for (String del : dels) {
            Delete delete = new Delete(Bytes.toBytes(uid));
            delete.addColumn(Bytes.toBytes(Constant.RELATION_TABLE_CF1), Bytes.toBytes(del));
            toDeletePut.add(delete);
        }
        //被关注的粉丝
        for (String del : dels) {
            Delete delete = new Delete(Bytes.toBytes(del));
            delete.addColumn(Bytes.toBytes(Constant.RELATION_TABLE_CF2), Bytes.toBytes(uid));
            toDeletePut.add(delete);
        }
        relationTable.delete(toDeletePut);

        //删除微博收件箱表数据
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX_TABLE));
        List<Delete> deletes = new ArrayList<>();
        for (String del : dels) {
            Delete delete = new Delete(Bytes.toBytes(uid));
            delete.addColumn(Bytes.toBytes(Constant.INBOX_TABLE_CF1), Bytes.toBytes(del));
            deletes.add(delete);
        }
        inboxTable.delete(deletes);

        inboxTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 获取用户的初始化页面数据
     *
     * @param uid
     */
    public static void getInit(String uid) throws IOException {

        Connection connection = ConnectionFactory.createConnection(HBaseUtil.getConfiguration());
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));

        Get get = new Get(Bytes.toBytes(uid));
//        get.addFamily(Bytes.toBytes(Constant.INBOX_TABLE_CF1));
        get.setMaxVersions();
        Result result = inboxTable.get(get);

        for (Cell cell : result.rawCells()) {
            byte[] bytes = CellUtil.cloneValue(cell);
            Get ge = new Get(bytes);
            Result result1 = contentTable.get(ge);

            for (Cell contentCell : result1.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(contentCell)) +
                        ", CF:" + Bytes.toString(CellUtil.cloneFamily(contentCell)) +
                        ", CN:" + Bytes.toString(CellUtil.cloneQualifier(contentCell)) +
                        ", Value:" + Bytes.toString(CellUtil.cloneValue(contentCell))
                );
            }
        }
        contentTable.close();
        inboxTable.close();
        connection.close();
    }

    /**
     * 获取某个人的微博详情
     *
     * @param uid
     */
    public static void getWeiBo(String uid) throws IOException {

        Connection connection = ConnectionFactory.createConnection(HBaseUtil.getConfiguration());
        Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));
        Scan scan = new Scan();
        //过滤扫描rowkey，即：前置位匹配被关注的人的uid_
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        //为扫描对象指定过滤规则
        scan.setFilter(filter);

        ResultScanner scanner = contentTable.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ", CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", Value:" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
        contentTable.close();
        connection.close();
    }

}
