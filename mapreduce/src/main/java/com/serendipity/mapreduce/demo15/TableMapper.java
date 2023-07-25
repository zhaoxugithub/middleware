package com.serendipity.mapreduce.demo15;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * fileSplit 继承 InputSplit
 */

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        if (filename.startsWith("order")) {
            String orderId = split[0];
            String productId = split[1];
            Integer amount = Integer.valueOf(split[2]);
            TableBean tableBean = new TableBean(orderId, productId, amount,"" , "order");
            context.write(new Text(productId), tableBean);
        } else {
            String productId = split[0];
            String productName = split[1];
            TableBean tableBean = new TableBean("", productId, 0,productName, "pd");
            context.write(new Text(productId), tableBean);
        }
    }
}
