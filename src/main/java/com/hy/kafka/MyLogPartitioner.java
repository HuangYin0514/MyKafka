package com.hy.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;


/**
 * Created with IDEA by User1071324110@qq.com
 *
 * @author 10713
 * @date 2018/7/5 21:40
 */
public class MyLogPartitioner implements Partitioner {

    public MyLogPartitioner(VerifiableProperties properties) {

    }

    public int partition(Object o, int i) {
        return 1;
    }
}
