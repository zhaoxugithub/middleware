package com.serendipity.mapreduce.demo06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CustomRecordReader extends RecordReader<Text, BytesWritable> {


    /**
     * hdfs目录中的文件
     */
    private FileSplit fileSplit;

    /**
     * 配置文件
     */
    private Configuration configuration;

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    /**
     * 定义flag
     *
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    private boolean flag = true;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    /**
     * 创建key-value
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (flag) {
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            //获取hdfs文件的路径
            Path path = fileSplit.getPath();
            key.set(path.toString());
            //借用FileSystem 打开文件流
            FileSystem fileSystem = FileSystem.get(configuration);
            FSDataInputStream open = fileSystem.open(path);
            IOUtils.readFully(open, bytes, 0, bytes.length);
            value.set(bytes, 0, bytes.length);
            flag = false;
            return true;
        }
        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
