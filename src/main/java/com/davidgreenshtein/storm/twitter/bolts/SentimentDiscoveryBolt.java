package com.davidgreenshtein.storm.twitter.bolts;

import com.davidgreenshtein.storm.twitter.sentiments.SentimentsRecognizer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class SentimentDiscoveryBolt extends BaseBasicBolt {

    public static String FIELD_NAME = "tweet";
    public static String OUTPUT_FIELD_NAME = "sentence";

    private SentimentsRecognizer sentimentsRecognizer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.sentimentsRecognizer = new SentimentsRecognizer();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(OUTPUT_FIELD_NAME));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String tweet = (String) tuple.getValueByField(FIELD_NAME);
        String line = (tweet != null) ? tweet.replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase() : null;

        if (line != null && ! line.isEmpty()) {
            String sentiment = sentimentsRecognizer.discoverSentiment(line);

            // continue with positive sentences only
            if ("Positive".equalsIgnoreCase(sentiment)) {
                basicOutputCollector.emit(new Values(line));
            }
        }
    }
}
