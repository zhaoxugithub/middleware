package com.serendipity.hbase.atguigu;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

@Slf4j
public class HbaseDDL {

    public static Connection connection = HbaseConnectSingleton.connection;

    /**
     * 创建命名空间
     *
     * @param nameSpace
     */
    public static void createNameSpace(String nameSpace) throws IOException {

        // 获取admin，用admin对象操作DDL
        // 此处的异常先不要抛出，等到方法写完，再统一抛出
        // admin的连接是轻量级的，不是线程安全的，不推荐池化或者缓存这个连接，用到就去获取，用不到就关闭
        // 这里需要抛出去，原因是因为这里是连接有问题，代码层面解决不了，抛出去
        Admin admin = connection.getAdmin();
        // 调用方法创建命名空间
        /*
            NamespaceDescriptor 无法被new出来，因为他的构造方法是私有的，
            所有NamespaceDescriptor 内部肯定有一个建造者，或者一个静态类把这个对象创建出来
            所以用create
         */
        // 创建命名空间描述建造者 设计师
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);
        // 给命名空间添加需求 键值对
        builder.addConfiguration("user", "atguigu");
        // 调用方法创建命名空间
        // 代码相对shell更加底层，所以shell能实现的功能，代码一定能够实现
        // 所以需要填写完成的命名空间描述
        // 创建命名空间出现的问题，都属于方法本身的问题，不应该抛出
        // 比如创建了已经存在的命名空间，但是不影响后续的代码
        try {

            admin.createNamespace(builder.build());
        } catch (IOException e) {
            log.info("命名空间已经存在 e ={}", e);
            System.out.println("命名空间已经存在");
            e.printStackTrace();
        }
        // 关闭admin
        admin.close();
    }


    /**
     * 判断表格是否存在
     *
     * @param namespace 命名空间
     * @param tableName 表格名称
     * @return
     */
    public static boolean isTableExists(String namespace, String tableName) throws IOException {
        // 获取admin
        Admin admin = connection.getAdmin();
        // 可以看到TableName 是私有的，无法直接new出来,所以只能通过valueOf方法
        // new TableName(namespace.getBytes(), tableName)
        boolean exists = false;
        try {
            exists = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            System.out.println("表格是否存在报异常");
            e.printStackTrace();
        }
        admin.close();
        return exists;
    }

    /**
     * 创建表
     *
     * @param namespace      命名空间名称
     * @param tableName      表名称
     * @param columnFamilies 列族名称 可以有几个
     */
    public static void createTable(String namespace, String tableName, String... columnFamilies) throws IOException {
        // 判断是否至少一个列族
        if (columnFamilies.length == 0) {
            System.out.println("创建表格至少有一个列族");
            return;
        }
        // 判断表格是否存在
        if (isTableExists(namespace, tableName)) {
            System.out.println("表格已经存在");
            return;
        }
        // 获取admin
        Admin admin = connection.getAdmin();
        // 调用方法创建表格
        // 创建表格描述的建造者
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));
        // 添加参数
        for (String columnFamily : columnFamilies) {
            // 创建列族描述的建造者
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            columnFamilyDescriptorBuilder.setMaxVersions(5);
            // 创建添加完参数的列族描述，底层是一个map
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        // 创建对应的表格描述
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
    }

    /**
     * 修改表
     *
     * @param namespace    命名空间
     * @param tableName    表名称
     * @param columnFamily 列族
     * @param version      版本
     */
    public static void modifyTable(String namespace, String tableName, String columnFamily, int version) throws IOException {
        // 判断表格是否存在
        if (!isTableExists(namespace, tableName)) {
            System.out.println("表格不存在");
            return;
        }
        // 获取admin
        Admin admin = connection.getAdmin();
        try {
            // 调用方法修改表格
            // 获取之前的表格描述
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(tableName));
            // 创建一个表格描述建造者,newBuilder 接受两种参数一种是TableDescriptor 或者是TableName
            // 如果使用tableName的方法，相当于创建了一个新的表格描述建造者，没有之前的信息
            // 如果想要修改之前的信息，必须调用方法填写一个旧的表格描述
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);
            // 对应建造者进行表格数据的修改
            ColumnFamilyDescriptor familyDescriptor = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));
            // 创建列族描述建造者
            // 需要填写旧的列族描述
            ColumnFamilyDescriptorBuilder newFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(familyDescriptor);
            // 修改对应的版本
            newFamilyDescriptor.setMaxVersions(version);
            // 此处修改的时候 如果填写的新创建，那么别的参数会初始化
            tableDescriptorBuilder.modifyColumnFamily(newFamilyDescriptor.build());
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 关闭admin
        admin.close();
        System.out.println("创建成功");
    }


    /**
     * 删除表
     *
     * @param namespace 命名空间
     * @param tableName 表名称
     * @return
     * @throws IOException
     */
    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        // 判断表格是否存在
        if (!isTableExists(namespace, tableName)) {
            System.out.println("表格不存在，无法删除");
            return false;
        }
        // 获取admin
        Admin admin = connection.getAdmin();
        // 调用相关的方法删除表格
        try {
            // Hbase 删除表格之前 一定要先标记表格为不可以
            TableName tableName1 = TableName.valueOf(namespace, tableName);
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
        return true;
    }

    public static void main(String[] args) throws IOException {
        // createNameSpace("atguigu");
        // // 当命名空间已经存在了，再次创建会抛出异常，然后导致System.out.println("其他代码。。。") 并没有执行
        // // 解决方法是捕获异常,然后就会继续往下执行
        // System.out.println("其他代码。。。");


        boolean tableExists = isTableExists("atguigu", "student");
        System.out.println("tableExists = " + tableExists);
        // // 关闭hbase连接
        createTable("atguigu", "student", "info", "msg");

        // boolean b = deleteTable("atguigu", "student");
        // System.out.println("b = " + b);
        //
        //
        boolean tableExists1 = isTableExists("atguigu", "student");
        System.out.println("tableExists1 = " + tableExists1);
        HbaseConnectSingleton.closeConnection();

    }
}
