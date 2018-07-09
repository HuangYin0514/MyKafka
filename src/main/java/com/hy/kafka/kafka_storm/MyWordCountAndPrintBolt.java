package com.hy.kafka.kafka_storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import scala.runtime.Int;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IDEA by User1071324110@qq.com
 *
 * @author 10713
 * @date 2018/7/9 9:51
 */
public class MyWordCountAndPrintBolt extends BaseBasicBolt {

    private Map<String, String> wordCountMap = new HashMap<String, String>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        //可以连接redis
        //jedis = new Jedis("127.0.0.1", 6379);
        super.prepare(stormConf, context);
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = (String) input.getValueByField("word");
        Integer num = (Integer) input.getValueByField("num");
        //1、查看单词对应的value是否存在
        Integer integer = wordCountMap.get(word) == null ? 0 : Integer.parseInt(wordCountMap.get("word"));
        if (integer == null || integer.intValue() == 0) {
            wordCountMap.put(word, num + "");
        } else {
            wordCountMap.put(word, (integer.intValue() + num) + "");
        }
        System.out.println(wordCountMap);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //todo 不需要要定义输出字段

    }
}
