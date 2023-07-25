package com.serendipity.hbase.weibo.constants;

/**
 * 定义常量
 */
public class Constant {
    /**
     * 命名空间
     */
    public static final String NameSpace = "weibo";
    /**
     * 微博内容表
     */
    public static String CONTENT_TABLE = "weibo:content";
    public static String CONTENT_TABLE_CF = "info";
    public static final int CONTENT_TABLE_VERSION = 1;
    /**
     * 用户关系表
     */
    public static String RELATION_TABLE = "weibo:relation";
    public static String RELATION_TABLE_CF1 = "attends";
    public static String RELATION_TABLE_CF2 = "fans";
    public static final int RELATION_TABLE_VERSION = 1;
    /**
     * 微博收件箱表
     */
    public static String INBOX_TABLE = "weibo:inbox";
    public static String INBOX_TABLE_CF1 = "info";
    public static final int INBOX_TABLE_VERSION = 1;
}
