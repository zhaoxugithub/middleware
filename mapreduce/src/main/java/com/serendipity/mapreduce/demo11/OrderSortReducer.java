package com.serendipity.mapreduce.demo11;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderSortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    /**
     * 每一个分组会调用一次reduce方法
     * 相同的key执行一次
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
