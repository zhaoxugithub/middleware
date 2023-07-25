package com.serendipity.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * hdfs的javaApi 操作
 */
public class HdfsCilentAPI {


    static {
        System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop");
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        testMkdirs();
//        testCopyFromLocalFile();
        //testCopyToLocalFile();
        //testDelete();
        //testRename();
        listFiles();
//        System.out.println("-------------");
//        testListFiles();
        // putFileToHDFS();
        //getFileFromHDFS();
        //readFileSeek1();
        //readFileSeek2();
        //testCopyFromLocalFile();
    }

    /**
     * 获取文件系统方法一
     *
     * @return
     */
    public static FileSystem getFileSystem01() {
        //为true表示要加载resource 里面的配位文件
        Configuration configuration = new Configuration(true);
        //指定我们使用的文件系统类型:
//        configuration.set("fs.defaultFS", "hdfs://hadoop01:9000");
        FileSystem fileSystem = null;
//        try {
//            fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "root");
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (URISyntaxException e) {
//            e.printStackTrace();
//        }
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    /**
     * 获取文件系统方法二
     *
     * @return
     */
    public static FileSystem getFileSystem02() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFs", "hdfs://CentOS201:9000");
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://CentOS201:9000"), configuration, "root");
        return fileSystem;
    }

    /**
     * 获取文件系统三
     *
     * @return
     */
    public static FileSystem getFileSystem03() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://CentOS201:9000"), new Configuration(), "root");
        return fileSystem;
    }

    /**
     * 获取文件系统四
     *
     * @return
     */
    public static FileSystem getFileSystem04() throws URISyntaxException, IOException, InterruptedException {
        return FileSystem.newInstance(new URI("hdfs://hadoop01:9000"), new Configuration(), "root");
    }

    /**
     * 创建文件目录
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    public static void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        FileSystem fileSystem = getFileSystem04();
        try {
            fileSystem.mkdirs(new Path("/serendipity/demo01"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            fileSystem.close();
        }

    }


    /**
     * 文件上传
     *
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    public static void testCopyFromLocalFile() throws IOException, URISyntaxException, InterruptedException {
        FileSystem fileSystem = getFileSystem01();
        fileSystem.copyFromLocalFile(new Path("C:\\Users\\Administrator\\Desktop\\flow.txt"), new Path("/zhaoxu/test04"));
        fileSystem.close();
    }

    /**
     * 文件下载
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = getFileSystem04();
        fileSystem.copyToLocalFile(new Path("/zhaoxu/test04/集群分配.txt"), new Path("C:\\Users\\Administrator\\Desktop\\test"));
        fileSystem.close();
    }


    /**
     * 测试删除
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void testDelete() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = getFileSystem01();
        boolean delete = fileSystem.delete(new Path("/zhaoxu/test04/集群分配.txt"), true);
        fileSystem.close();
    }

    /**
     * 修改名称
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void testRename() throws URISyntaxException, IOException, InterruptedException {

        FileSystem fileSystem = getFileSystem01();
        boolean rename = fileSystem.rename(new Path("/zhaoxu/test04/README.md"), new Path("/zhaoxu/test04/test.md"));
        fileSystem.close();
    }

    /**
     * 获取文件目录
     * <p>
     * <p>
     * LocatedFileStatus 是 FileStatus 的子类，LocatedFileStatus可以获取每一个块的详细信息
     * FileStatus 只能获取每一个文件的相信信文件的元数据信息
     * {
     * "path":"hdfs://CentOS201:9000/user",
     * "isDirectory":"true",
     * "modification_time":1596996758268,
     * "access_time":0,
     * "owner":"root",
     * "group":"supergroup",
     * "permission":"rwxr-xr-x",
     * "isSymlink":"false"
     * }
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void listFiles() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = getFileSystem01();
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            //todo 每一个文件的属性
            if (fileStatus.isDirectory()) {

            }
            System.out.println(fileStatus);
        }
        fileSystem.close();
    }


    /**
     * 获取每个块的详情信息
     * {
     * "path":"hdfs://CentOS201:9000/HBase/MasterProcWALs/action_design.state-00000000000000000004.log",
     * "isDirectory":"false",
     * "length":30,
     * "replication":4,
     * "blocksize":134217728,
     * "modification_time":1597001205280,
     * "access_time":1596997648878,
     * "owner":"root",
     * "group":"supergroup",
     * "permission":"rw-r--r--",
     * "isSymlink":"false"
     * }
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = getFileSystem01();
        //获取文件系统下面的所有文件
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
                fileSystem.listFiles(new Path("/"), true);
        //遍历每一个文件
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
//            System.out.println(locatedFileStatus);
            //todo locatedFileStatus 对象包含文件的所有属性
            //获取每一个文件块个数
            long blockSize = locatedFileStatus.getBlockSize();
            //输出块的大小
            //System.out.println(blockSize / 1024 / 1024);
            //获取每一个文件的文件块数组
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for (BlockLocation location : blockLocations) {
                //todo 获取每一个文件块的属性
                //获取每一个文件块的备份所在的主机
                String[] hosts = location.getHosts();
                System.out.println(Arrays.toString(hosts));
            }
        }
        fileSystem.close();
    }


    /**
     * 把本地文件上传到hdfs 采用流的方式
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = getFileSystem01();
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/zhaoxu/test11/11.md"), true);
        FileInputStream inputStream = new FileInputStream(new File("C:\\Users\\Administrator\\Desktop\\README.md"));
        IOUtils.copyBytes(inputStream, fsDataOutputStream, fileSystem.getConf());
        inputStream.close();
        fsDataOutputStream.close();
        fileSystem.close();
    }

    /**
     * 采用流的方式 将hdfs上的文件下载到本地
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = getFileSystem01();
        FSDataInputStream open = fileSystem.open(new Path("/zhaoxu/test11/11.md"));
        FileOutputStream outputStream = new FileOutputStream(new File("C:\\Users\\Administrator\\Desktop\\README22.md"));
        ////hdfs里面自带的拷贝流方法
        IOUtils.copyBytes(open, outputStream, fileSystem.getConf());
        open.close();
        outputStream.close();
        fileSystem.close();
    }

    /**
     * 分块读取HDFS上的大文件，比如根目录下的/hadoop-2.7.2.tar.gz
     * 上传第一个块
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = getFileSystem01();
        FSDataInputStream open = fileSystem.open(new Path("/hadoop-2.7.2.tar.gz"));
        FileOutputStream outputStream = new FileOutputStream("C:\\Users\\Administrator\\Desktop\\hadoop-2.7.2.tar.gz.part1");

        byte[] bytes = new byte[1024];

        for (int i = 0; i < 128 * 1024; i++) {
            open.read(bytes);
            outputStream.write(bytes);
        }
        open.close();
        outputStream.close();
        fileSystem.close();
    }

    /**
     * 上传第二个块
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = getFileSystem01();
        FSDataInputStream open = fileSystem.open(new Path("/hadoop-2.7.2.tar.gz"));
        FileOutputStream outputStream = new FileOutputStream("C:\\Users\\Administrator\\Desktop\\hadoop-2.7.2.tar.gz.part2");
        open.seek(1024 * 1024 * 128);
        IOUtils.copyBytes(open, outputStream, fileSystem.getConf());
        open.close();
        outputStream.close();
        fileSystem.close();
    }
}
