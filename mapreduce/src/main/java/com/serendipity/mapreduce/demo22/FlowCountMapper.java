package com.serendipity.mapreduce.demo22;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class FlowCountMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    TreeMap<FlowBean, Text> resultMap = new TreeMap<FlowBean, Text>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");

        String phone = split[0];
        Long upFlow = Long.valueOf(split[1]);
        Long downFlow = Long.valueOf(split[2]);
        FlowBean flowBean = new FlowBean(upFlow, downFlow);

        resultMap.put(flowBean, new Text(phone));

        if (resultMap.size() > 10) {
            resultMap.remove(resultMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Iterator<FlowBean> iterator = resultMap.keySet().iterator();

        while (iterator.hasNext()) {
            FlowBean next = iterator.next();
            context.write(next, resultMap.get(next));
        }

    }
}
