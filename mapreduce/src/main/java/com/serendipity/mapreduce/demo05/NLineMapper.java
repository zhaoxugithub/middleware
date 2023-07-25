package com.serendipity.mapreduce.demo05;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split(" ");
        for (String str : values) {
            context.write(new Text(str), new LongWritable(1));
        }
    }
}
