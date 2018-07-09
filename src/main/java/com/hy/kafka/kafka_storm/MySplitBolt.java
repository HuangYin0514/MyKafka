package com.hy.kafka.kafka_storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created with IDEA by User1071324110@qq.com
 *
 * @author 10713
 * @date 2018/7/9 9:48
 */
public class MySplitBolt extends BaseBasicBolt {

    public void execute(Tuple input, BasicOutputCollector collector) {
        byte[] juzi = (byte[]) input.getValueByField("bytes");
        String[] split = new String(juzi).split(" ");
        for (String word : split) {
            collector.emit(new Values(word, 1));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "num"));
    }

}
