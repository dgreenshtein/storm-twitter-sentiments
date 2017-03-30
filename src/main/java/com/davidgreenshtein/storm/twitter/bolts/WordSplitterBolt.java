package com.davidgreenshtein.storm.twitter.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class WordSplitterBolt extends BaseBasicBolt {

    public static String FIELD_NAME = "sentence";
    public static String OUTPUT_FIELD_NAME = "word";
    private static String SPLIT_BY = " ";

    private final int minWordLength;

    public WordSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(OUTPUT_FIELD_NAME));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String tweetText = (String) tuple.getValueByField(FIELD_NAME);
        String[] words = (tweetText != null) ? tweetText.split(SPLIT_BY) : new String[0];
        for (String word : words) {
            if (word.length() >= minWordLength) {
                basicOutputCollector.emit(new Values(word));
            }
        }
    }
}
