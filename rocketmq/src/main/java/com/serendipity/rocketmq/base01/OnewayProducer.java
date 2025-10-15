package com.serendipity.rocketmq.base01;

import com.serendipity.rocketmq.Constant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/*
producer.sendOneway 发送消息时不会等待服务器响应，也不会返回任何结果，
适用于对可靠性要求不高、无需回执的场景，性能较高。
producer.send 发送消息时会等待服务器响应，并返回发送结果（如消息ID、发送状态等），
适用于需要确认消息是否成功发送的场景，可靠性更高但性能略低。
 */
public class OnewayProducer {
    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("OnewayProducer_group");
        // 设置NameServer的地址
        producer.setNamesrvAddr(Constant.NAME_SERVER);
        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 100; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("base", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 发送单向消息，没有任何返回结果
            producer.sendOneway(msg);
        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
