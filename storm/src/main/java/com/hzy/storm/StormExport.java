package com.hzy.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.hzy.storm.StormTransform.KafkaWordToUpperCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * Created by zy on 2016/1/7.
 * Storm输出端
 */
public class StormExport {

    public static class RealtimeBolt extends BaseRichBolt {

        private static final Log LOG = LogFactory.getLog(KafkaWordToUpperCase.class);
        private static final long serialVersionUID = -4115132557403913367L;
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String line = input.getString(0).trim();
            LOG.info("REALTIME: " + line);
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
