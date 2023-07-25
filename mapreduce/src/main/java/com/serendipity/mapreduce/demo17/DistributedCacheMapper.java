package com.serendipity.mapreduce.demo17;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> productMap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1 获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "utf-8"));
        String line;
        while (StringUtils.isNotEmpty((line = bufferedReader.readLine()))) {
            String[] split = line.split("\t");
            String productId = split[0];
            String productName = split[1];
            productMap.put(productId, productName);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String productId = split[1];
        String productName = productMap.get(productId);
        String out = value + "\t" + productName;
        context.write(new Text(out), NullWritable.get());
    }
}
