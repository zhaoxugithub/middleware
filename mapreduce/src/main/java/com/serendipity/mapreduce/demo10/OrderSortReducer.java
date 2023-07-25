package com.serendipity.mapreduce.demo10;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderSortReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    /**
     * 每一个分组会调用一次reduce方法
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
