package com.serendipity.mapreduce.demo02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String phone = split[1];
        Long upFlow = Long.valueOf(split[5]);
        Long downFlow = Long.valueOf(split[5]);
        FlowBean flowBean = new FlowBean(upFlow, downFlow);
        context.write(flowBean, new Text(phone));
    }
}
