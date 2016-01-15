package com.hzy;

/**
 * Created by Hzy on 2015/12/29.
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.hzy.storm.StormTransform.KafkaWordToUpperCase;
import com.hzy.storm.StormExport.RealtimeBolt;
import com.hzy.kafka.KafkaExtract;
import com.hzy.hdfs.HdfsExport;
import storm.kafka.*;

public class DistributeWordTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        KafkaExtract kafkaExtract = new KafkaExtract();
        HdfsExport hdfsExport = new HdfsExport();
        SpoutConfig spoutConf = kafkaExtract.KafkaConfig("master,slave1,slave2", "test", "/storm", "word");

        // configure & build topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 2);
        builder.setBolt("to-upper", new KafkaWordToUpperCase(), 2).shuffleGrouping("kafka-reader");
        builder.setBolt("hdfs-bolt", hdfsExport.HdfsConfig(), 2).shuffleGrouping("to-upper");
        builder.setBolt("realtime", new RealtimeBolt(), 2).shuffleGrouping("to-upper");

        // submit topology
        Config conf = new Config();
        String name = DistributeWordTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            String nimbus = args[0];
            conf.put(Config.NIMBUS_HOST, nimbus);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(3600000);
            cluster.shutdown();
        }
    }
}
