package com.serendipity.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class CustomSink extends AbstractSink implements Configurable {
    private String prefix = null;
    private String suffix = null;

    @Override
    public Status process() throws EventDeliveryException {
        // 申明返回值状态信息
        Status status;
        // 获取当前sink绑定的Channel
        Channel channel = getChannel();
        // 获取事务
        Transaction transaction = channel.getTransaction();
        // 申明事件
        Event event;
        // 开启事务
        transaction.begin();
        while (true) {
            event = channel.take();
            if (event != null) {
                break;
            }
        }
        try {
            // 打印事件
            System.out.println(prefix + new String(event.getBody()) + suffix);
            // 事务提交
            transaction.commit();
            status = Status.READY;
        } catch (Exception e) {
            // 遇到异常，事务回滚
            transaction.rollback();
            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix", "zhaoxu");
        suffix = context.getString("suffix", "zhaoxu");
    }
}
