package com.serendipity.mapreduce.demo20;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 数据流的压缩和解压缩
 */

public class TestCompress {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        compress("C:\\Users\\Administrator\\Desktop\\test\\etlsimple\\web.log",
//                "org.apache.hadoop.io.compress.BZip2Codec");
        decompress("C:\\Users\\Administrator\\Desktop\\test\\etlsimple\\web.log.bz2");
    }

    /**
     * 压缩
     *
     * @param filename
     * @param method
     */
    private static void compress(String filename, String method) throws IOException, ClassNotFoundException {
        // （1）获取输入流
        FileInputStream fileInputStream = new FileInputStream(filename);
        //获取解压缩对象
        Class<?> compressMethod = Class.forName(method);
        CompressionCodec com = (CompressionCodec) ReflectionUtils.newInstance(compressMethod, new Configuration());
        // （2）获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("C:\\Users\\Administrator\\Desktop\\test\\etlsimple\\web.log" + com.getDefaultExtension()));
        CompressionOutputStream outputStream = com.createOutputStream(fileOutputStream);
        // （3）流的对拷  true 表示开启流关闭操作
        IOUtils.copyBytes(fileInputStream, outputStream, new Configuration(), true);
    }

    /**
     * 解压缩
     *
     * @param filename
     */
    private static void decompress(String filename) throws IOException {

        //校验文件是否可以解压缩
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null) {
            //todo 抛出异常
            return;
        }
        //获取输入流
        CompressionInputStream inputStream = codec.createInputStream(new FileInputStream(new File(filename)));
        //获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File("C:\\Users\\Administrator\\Desktop\\test\\etlsimple\\web2.log"));
        //流的对拷
        IOUtils.copyBytes(inputStream, outputStream, new Configuration(), true);
    }
}
