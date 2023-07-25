package com.serendipity.hbase.action;//package com.hbase.action;
//
//
//import com.hbase.util.HBaseConfig;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//public class TableAction {
//
//    /**
//     * 判断表是否存在
//     *
//     * @param tableName
//     * @return
//     * @throws IOException
//     */
//    public static boolean isTableExist(String tableName) throws IOException {
//        Connection connection = ConnectionFactory.createConnection(HBaseConfig.getConfiguration());
//        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
//        return admin.tableExists(tableName);
//    }
//
//    /**
//     * 创建表
//     *
//     * @param tableName
//     * @param columnFamily
//     * @throws IOException
//     */
//    public static void createTable(String tableName, String... columnFamily) throws IOException {
//
//        if (isTableExist(tableName)) {
//            System.out.printf("该表" + tableName + "已经存在");
//            System.exit(0);
//        }
//        HBaseAdmin admin = new HBaseAdmin(HBaseConfig.getConfiguration());
//
//        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
//
//        for (String col : columnFamily) {
//            descriptor.addFamily(new HColumnDescriptor(col));
//        }
//        admin.createTable(descriptor);
//        System.out.println("表" + tableName + "创建成功");
//    }
//
//    /**
//     * 删除表
//     *
//     * @param tableName
//     */
//    public static void dropTable(String tableName) throws IOException {
//        HBaseAdmin admin = new HBaseAdmin(HBaseConfig.getConfiguration());
//        if (isTableExist(tableName)) {
//            //删除表之前disable表
//            admin.disableTable(tableName);
//            //删除表
//            admin.deleteTable(tableName);
//            System.out.println("表" + tableName + "已经删除");
//        } else {
//            System.out.println("表" + tableName + "不存在");
//        }
//    }
//
//    /**
//     * 添加数据
//     *
//     * @param tableName
//     * @param rowKey
//     * @param columnFamily
//     * @param column
//     * @param value
//     * @throws IOException
//     */
//    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
//
//        Configuration configuration = HBaseConfig.getConfiguration();
//        //创建HTable对象
//        HTable hTable = new HTable(configuration, tableName);
//        //向表中插入数据
//        Put put = new Put(Bytes.toBytes(rowKey));
//        //向Put对象里面组装数据
//        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
//        hTable.put(put);
//        hTable.close();
//        System.out.println("插入数据成功");
//    }
//
//    /**
//     * 删除多行数据
//     *
//     * @param tableName
//     * @param rows
//     */
//    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
//
//        Configuration configuration = HBaseConfig.getConfiguration();
//        HTable table = new HTable(configuration, tableName);
//        List<Delete> deleteList = new ArrayList<Delete>();
//        for (String row : rows) {
//            Delete delete = new Delete(row.getBytes());
//            deleteList.add(delete);
//        }
//        table.delete(deleteList);
//        table.close();
//    }
//
//    /**
//     * 获取所有数据
//     *
//     * @param tableName
//     * @throws IOException
//     */
//    public static void getAllRows(String tableName) throws IOException {
//
//        Configuration configuration = HBaseConfig.getConfiguration();
//        HTable hTable = new HTable(configuration, tableName);
//
//        //得到用于扫描region的对象
//        Scan scan = new Scan();
//
//        //使用HTable得到resultCanNer实现类的对象
//        ResultScanner resultScanner = hTable.getScanner(scan);
//
//        for (Result result : resultScanner) {
//            Cell[] cells = result.rawCells();
//            for (Cell cell : cells) {
//                //得到rowKey
//                byte[] bytes = CellUtil.cloneRow(cell);
//                String rowKey = Bytes.toString(bytes);
//                System.out.println("行键：" + rowKey);
//
//                //得到列族
//                byte[] bytes1 = CellUtil.cloneFamily(cell);
//                String colFamily = Bytes.toString(bytes1);
//                System.out.println("列族：" + colFamily);
//
//                //得到列
//                byte[] bytes2 = CellUtil.cloneQualifier(cell);
//                String col = Bytes.toString(bytes2);
//                System.out.println("列" + col);
//
//                //得到值
//                byte[] bytes3 = CellUtil.cloneValue(cell);
//                String value = Bytes.toString(bytes3);
//                System.out.println("值" + value);
//
//                System.out.println("时间戳：" + cell.getTimestamp());
//            }
//        }
//    }
//
//    /**
//     * 获取某一行数据
//     *
//     * @param tableName
//     * @param rowKey
//     * @throws IOException
//     */
//    public static void getRow(String tableName, String rowKey) throws IOException {
//        Configuration configuration = HBaseConfig.getConfiguration();
//        HTable table = new HTable(configuration, tableName);
//
//        Get get = new Get(Bytes.toBytes(rowKey));
//        //get.setMaxVersions();显示所有版本
//        //get.setTimeStamp();显示指定时间戳的版本
//        Result result = table.get(get);
//        for (Cell cell : result.rawCells()) {
//            System.out.println("行键:" + Bytes.toString(result.getRow()));
//            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
//            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
//            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
//            System.out.println("时间戳:" + cell.getTimestamp());
//        }
//    }
//
//    /**
//     * 获取某一行指定“列族:列”的数据
//     */
//    public static void getRowQualifier(String tableName, String rowKey, String colFamily, String qualifier) throws IOException {
//        Configuration configuration = HBaseConfig.getConfiguration();
//
//        HTable hTable = new HTable(configuration, tableName);
//
//        Get get = new Get(Bytes.toBytes(rowKey));
//
//        get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(qualifier));
//        Result result = hTable.get(get);
//        //猜测：应该有不同的版本号，所以需要遍历
//        for (Cell cell : result.rawCells()) {
//            System.out.println("行键:" + Bytes.toString(result.getRow()));
//            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
//            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
//            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
//            System.out.println("时间戳:" + cell.getTimestamp());
//        }
//
//
//    }
//
//    public static void main(String[] args) throws IOException {
//      createTable("table01", "info_1", "info_2");
////        for (int i = 0; i < 10; i++) {
////            addRowData("table01", "1002" + i, "info_1", "col_1", "value" + i);
////            addRowData("table01", "1002" + i, "info_2", "col_2", "value" + i);
////        }
////        getAllRows("table01");
//    }
//}
