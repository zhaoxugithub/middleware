package com.serendipity.mapreduce.demo16;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadMySqlMapper extends Mapper<LongWritable, OrderOutBean, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, OrderOutBean value, Context context) throws IOException, InterruptedException {
        context.write(new Text(value.toString()), NullWritable.get());
    }
}
