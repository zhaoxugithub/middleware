package com.serendipity.hbase.mr1;//package com.hbase.mr1;
//
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.mapreduce.TableReducer;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//
//import java.io.IOException;
//
//public class WriteFruitMRReducer extends TableReducer<LongWritable, Text, LongWritable> {
//
//    @Override
//    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        for (Text text : values) {
//            String[] fields = text.toString().split("\t");
//            Put put = new Put(Bytes.toBytes(fields[0]));
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));
//            context.write(key, put);
//        }
//    }
//}
