package com.serendipity.hbase.mr2;//package com.hbase.mr2;
//
//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.CellUtil;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.IOException;
//
//public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {
//
//
//    //ImmutableBytesWritable 表示rowKey 的包装
//    @Override
//    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
//
//        Put put = new Put(key.get());
//        Cell[] cells = value.rawCells();
//
//        for (Cell cell : cells) {
//            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
//                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
//                    put.add(cell);
//                } else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
//                    put.add(cell);
//                }
//            }
//        }
//        context.write(key, put);
//    }
//}
