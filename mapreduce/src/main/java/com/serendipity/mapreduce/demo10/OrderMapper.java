package com.serendipity.mapreduce.demo10;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        int orderId = Integer.valueOf(split[0]);
        double price = Double.valueOf(split[2]);
        context.write(new IntWritable(orderId), new DoubleWritable(price));
    }
}
