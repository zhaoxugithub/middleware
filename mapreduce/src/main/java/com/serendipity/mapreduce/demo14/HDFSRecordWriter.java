package com.serendipity.mapreduce.demo14;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSRecordWriter extends RecordWriter<Text, IntWritable> {

    private FileSystem fileSystem = null;
    private FSDataOutputStream fsDataOutputStream = null;
    private StringBuffer sb = new StringBuffer();

    public static FileSystem getFileSystem01() {
        Configuration configuration = new Configuration();
        //指定我们使用的文件系统类型:
        configuration.set("fs.defaultFS", "hdfs://CentOS201:9000");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://CentOS201:9000"), configuration, "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    public HDFSRecordWriter() {
        fileSystem =getFileSystem01();
        try {
            fsDataOutputStream = fileSystem.create(new Path("/zhaoxu/test/mapreduce/out/word.txt"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void write(Text text, IntWritable intWritable) throws IOException, InterruptedException {
        String result = text.toString() + '\t' + String.valueOf(intWritable) + "\r\n";
        sb.append(result);
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sb.toString().getBytes());
        IOUtils.copyBytes(byteArrayInputStream, fsDataOutputStream, fileSystem.getConf());
        byteArrayInputStream.close();
        fsDataOutputStream.close();
        fileSystem.close();
    }
}
