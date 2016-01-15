package com.hzy.kafka;


import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.Arrays;

/**
 * Created by zy on 2016/1/7.
 */
public class KafkaExtract {

    public SpoutConfig KafkaConfig(String zks,String topic,String zkRoot,String id){
        // Configure Kafka
        //String zks = "master,slave1,slave2";
        //String topic = "test";
        //String zkRoot = "/storm"; // default zookeeper root configuration for storm
        //String id = "word";
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = true;
        spoutConf.zkServers = Arrays.asList(new String[]{"master,slave1,slave2"});
        spoutConf.zkPort = 2181;
        return  spoutConf;
    }
}
