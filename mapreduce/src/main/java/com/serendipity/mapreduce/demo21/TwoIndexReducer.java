package com.serendipity.mapreduce.demo21;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        for (Text text : values) {
            String val = text.toString().replace("\t", "-->") + "\t";
            sb.append(val);
        }
        context.write(key, new Text(sb.toString()));
    }
}
