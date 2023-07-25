package com.serendipity.hbase.weibo.test;


import com.serendipity.hbase.weibo.constants.Constant;
import com.serendipity.hbase.weibo.dao.HBaseDao;
import com.serendipity.hbase.weibo.utils.HBaseUtil;

import java.io.IOException;

public class TestWeiBo {

    public static void init() {

        try {
            //创建命名空间
            HBaseUtil.createNameSpace(Constant.NameSpace);

            //创建微博内容表
            HBaseUtil.createTable(Constant.CONTENT_TABLE, Constant.CONTENT_TABLE_VERSION, Constant.CONTENT_TABLE_CF);

            //创建微博用户关系表
            HBaseUtil.createTable(Constant.RELATION_TABLE, Constant.RELATION_TABLE_VERSION, Constant.RELATION_TABLE_CF1, Constant.RELATION_TABLE_CF2);

            //创建微博收件箱表
            HBaseUtil.createTable(Constant.INBOX_TABLE, Constant.INBOX_TABLE_VERSION, Constant.INBOX_TABLE_CF1);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        init();
        //1001 发布微博
        HBaseDao.publishWeiBo("1001", "赶紧下课啊");
        //1002 关注1001和1003
        HBaseDao.addAttends("1002", "1001", "1003");

        //获取1002 初始化页面
        HBaseDao.getInit("1002");

        System.out.println("********************111******************");
        //1003 发布3条微博，同事1001 发布两条微博
        HBaseDao.publishWeiBo("1003", "谁说的赶紧下课");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1001", "我没有说话");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1003", "那谁说的");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1001", "反正飞机是下线了");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1003", "你们爱咋的咋的");
        Thread.sleep(10);
        //获取1002的初始化页面
        HBaseDao.getInit("1002");
        System.out.println("********************222******************");
        //1002 取关1003
        HBaseDao.deleteAttends("1002", "1003");

        //获取1002的初始化页面
        HBaseDao.getInit("1002");
        System.out.println("********************333******************");
        //1002再次关注1003
        HBaseDao.addAttends("1002", "1003");

        //获取1002的初始化页面
        HBaseDao.getInit("1002");
        System.out.println("********************444******************");
        //获取1001的详情
        HBaseDao.getWeiBo("1001");
    }
}
