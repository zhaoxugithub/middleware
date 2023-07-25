package com.serendipity.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HDFSClientAPI2 {
    static {
        System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop");
    }
    private static Configuration configuration = new Configuration(true);
    private static FileSystem fs = null;
    public static void main(String[] args) throws IOException {
        getFileSystem();
//        uploadToHdfs();
//        createFile();
//        createDir();
//        listDirsOrFiles();
//        removeFile();
//        readBlock();
//        upFromLocal();
        downLoadLocal();
    }

    public static void getFileSystem() {
        try {
            fs = FileSystem.get(new URI("/serendipity"), configuration, "root");
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    // 创建目录
    public static void createDir() throws IOException {
        fs.mkdirs(new Path("/serendipity/demo02"));
    }

    // 创建文件
    public static void createFile01() throws IOException {
        // 上传文件，如果目录不存在就会创建
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/serendipity/demo03/1.txt"));
        fsDataOutputStream.writeBytes("Hello World");
        fsDataOutputStream.flush();
    }


    // 将本地文件通过IO流的方式上传到HDFS上
    public static void upFromLocal() throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/serendipity/demo03/2.txt"));
        FileInputStream fileInputStream = new FileInputStream("D:\\desktop\\新建文本文档.txt");
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, fs.getConf());
        fileInputStream.close();
        fsDataOutputStream.close();
    }

    // 上传到hdfs
    public static void uploadToHdfs() throws IOException {
        fs.copyFromLocalFile(new Path("D:\\soft\\hadoop-3.2.2.tar.gz"), new Path("/serendipity/demo04/"));
    }


    // 分块上传
    public static void uploadBlock() {

    }


    // 删除文件
    public static void removeFile() throws IOException {
        fs.deleteOnExit(new Path("/serendipity/demo2"));
    }

    // 列出目录
    public static void listDirsOrFiles() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/serendipity/"));
        Arrays.stream(fileStatuses).forEach((x) -> {
            if (x.isFile()) {
                System.out.println("file = " + x);
            } else {

                System.out.println("dir =" + x);
            }
        });
    }

    // 随机读取块文件
    public static void readBlock() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/serendipity/demo04"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println("fileStatus = " + fileStatus);
            long blockSize = fileStatus.getBlockSize();
            System.out.println("blockSize  = " + blockSize / 1024 / 1024);
            short replication = fileStatus.getReplication();
            System.out.println("replication = " + replication);
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("blockLocations.length = " + blockLocations.length);
            Arrays.stream(blockLocations).forEach(System.out::println);
        }
    }

    // 下载到本地
    public static void downLoadLocal() throws IOException {
        fs.copyToLocalFile(new Path("/serendipity/demo03/2.txt"), new Path("D:\\document\\idea\\hadoop_01\\hdfs\\src\\main\\java\\com\\hdfs\\"));
    }

    public static void getFromHdfs() {

    }

    public static void getFromHdfsByIO() {
    }
}
