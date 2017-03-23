package com.davidgreenshtein.storm.twitter.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class IgnoreWordsBolt extends BaseBasicBolt {

    private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList(new String[] {
            "http", "https", "the", "you", "que", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get",
            "about", "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
            "want", "will", "know", "good", "from", "las", "don", "people", "got", "why", "con", "time", "would",
    }));

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = (String) tuple.getValueByField("word");
        if (!IGNORE_LIST.contains(word)) {
            basicOutputCollector.emit(new Values(word));
        }
    }
}
