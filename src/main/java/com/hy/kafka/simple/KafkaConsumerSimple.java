package com.hy.kafka.simple;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IDEA by User1071324110@qq.com
 *
 * @author 10713
 * @date 2018/7/5 21:42
 */
public class KafkaConsumerSimple implements Runnable {
    public String title;
    public KafkaStream<byte[], byte[]> stream;

    public KafkaConsumerSimple(String title, KafkaStream<byte[], byte[]> stream) {
        this.title = title;
        this.stream = stream;
    }

    public void run() {
        System.out.println("开始运行 " + title);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        /**
         * 不停地从stream读取新到来的消息，在等待新的消息时，hasNext()会阻塞
         * 如果调用 `ConsumerConnector#shutdown`，那么`hasNext`会返回false
         * */
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> data = it.next();
            String topic = data.topic();
            int partition = data.partition();
            long offset = data.offset();
            String msg = new String(data.message());
            System.out.println(String.format("Consumer : [%s], Topic : [%s] , PartitionID :[%d], Offset:[%d],msg :[%s]", title, topic, partition, offset, msg));
        }
        System.out.println(String.format("Consumer :[%s] exiting ...", title));
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("group.id", "dashujujiagoushi");
        props.put("zookeeper.connect", "mini2:2181,mini3:2181");
        props.put("auto.offset.reset", "largest");
        //consumer向zookeeper提交offset的频率
        props.put("auto.commit.interval.ms", "1000");
        //分配策略  该种策略为每组中线程消费数目相同
        props.put("partition.assignment.strategy", "roundrobin");
        ConsumerConfig config = new ConsumerConfig(props);

        String topic1 = "orderMQ";
        //只要ConsumerConnector还在的话，consumer会一直等待新消息，不会自己退出
        ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(config);
        //定义一个map
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic1, 4);

        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamMap = javaConsumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = topicStreamMap.get(topic1);
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        for (int i = 0; i < streams.size(); i++) {
            executorService.execute(new KafkaConsumerSimple("消费者" + (i + 1), streams.get(i)));
        }
    }
}
