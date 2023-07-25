package com.serendipity.mapreduce.demo11;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        int orderId = Integer.valueOf(split[0]);
        double price = Double.valueOf(split[2]);
        context.write(new OrderBean(orderId, price), NullWritable.get());
    }
}
