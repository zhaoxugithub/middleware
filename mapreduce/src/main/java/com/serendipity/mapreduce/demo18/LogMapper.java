package com.serendipity.mapreduce.demo18;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        boolean result = parseLog(line, context);
        if (!result) {
            return;
        }
        context.write(new Text(line), NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] split = line.split(" ");
        if (split.length < 11) {
            context.getCounter("LogFilter", "remove").increment(1);
            return false;
        } else {
            context.getCounter("LogFilter", "remain").increment(1);
            return true;
        }
    }
}
