package com.davidgreenshtein.storm.twitter;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by davidgreenshtein on 23.03.17.
 */
public class RandomSentenceTestSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(RandomSentenceTestSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public RandomSentenceTestSpout() {
        this(true);
    }

    public RandomSentenceTestSpout(boolean isDistributed) {
        this._isDistributed = isDistributed;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
    }

    public void close() {
    }

    public void nextTuple() {
        Utils.sleep(100L);
        String[] words = new String[]{"there is a greate weather today", "do you have a nice mood?", "spring is coming"};
        Random rand = new Random();
        String word = words[rand.nextInt(words.length)];
        this._collector.emit(new Values(new Object[]{word}));
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new String[]{"tweet"}));
    }

    public Map<String, Object> getComponentConfiguration() {
        if(!this._isDistributed) {
            HashMap ret = new HashMap();
            ret.put("topology.max.task.parallelism", Integer.valueOf(1));
            return ret;
        } else {
            return null;
        }
    }
}



