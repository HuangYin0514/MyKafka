package com.hy.kafka.kafka_storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created with IDEA by User1071324110@qq.com
 *
 * @author 10713
 * @date 2018/7/9 9:29
 */
public class StormTopologyDriver {
    public static void main(String[] args) {
        //任务信息
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("KafkaSpout", new KafkaSpout(new SpoutConfig(new ZkHosts("mini2:2181"), "kafkaTest1", "/kafka", "kafkaTest1")), 2);
        topologyBuilder.setBolt("bolt1", new MySplitBolt(), 4).shuffleGrouping("KafkaSpout");
        topologyBuilder.setBolt("bolt2", new MyWordCountAndPrintBolt(), 2).shuffleGrouping("bolt");

        //任务提交
        Config config = new Config();
        config.setNumWorkers(2);
        StormTopology topology = topologyBuilder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordcount", config, topology);
    }
}
