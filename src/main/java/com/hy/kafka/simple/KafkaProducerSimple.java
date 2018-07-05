package com.hy.kafka.simple;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * Created with IDEA by User1071324110@qq.com
 *
 * @author 10713
 * @date 2018/7/5 21:43
 */
public class KafkaProducerSimple {
    public static void main(String[] args) {
        String TOPIC = "orderMQ";
        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", "mini1:9092,mini2:9092,mini3:9092");
        properties.put("request.required.acks", "1");
        properties.put("partitioner.class", "com.hy.kafka.MyLogPartitioner");
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(properties));
        for (int messageNo = 0; messageNo < 100000; messageNo++) {
            String messageStr = new String(messageNo + "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发注意：这里需要指定 partitionKey，用来配合自定义的MyLogPartitioner进行数据分发" +
                    "用来配合自定义的MyLogPartitioner进行数据分发");
            KeyedMessage<String, String> stringStringKeyedMessage = new KeyedMessage<String, String>(TOPIC, messageNo + "", "appid" + UUID.randomUUID() + messageStr);
            producer.send(stringStringKeyedMessage);
        }
    }
}
