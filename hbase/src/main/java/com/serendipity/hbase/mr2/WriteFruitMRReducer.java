package com.serendipity.hbase.mr2;//package com.hbase.mr2;
//
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableReducer;
//import org.apache.hadoop.io.NullWritable;
//
//import java.io.IOException;
//
//public class WriteFruitMRReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
//
//    @Override
//    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
//        for (Put put : values) {
//            context.write(NullWritable.get(), put);
//        }
//    }
//}
