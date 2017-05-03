package com.davidgreenshtein.storm.twitter.bolts;

import com.davidgreenshtein.storm.twitter.config.PropertiesHandler;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class SlidingWindowWordsCounterBolt extends BaseWindowedBolt {

    public static String FIELD_NAME = "word";
    private static String LOG_DELIMITER = "|";
    private static Integer TOP_N_DEFAULT=10;
    private Integer TOP_N;

    private OutputCollector collector;
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowWordsCounterBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.TOP_N = (Integer) stormConf.getOrDefault(PropertiesHandler.TOP_N_NUMBER, TOP_N_DEFAULT);
    }

    @Override
    public void execute(TupleWindow tupleWindow) {

        List<Tuple> tuplesInWindow = tupleWindow.get();

        // extract words in the current window
        List<String> collectedFields = tuplesInWindow.stream()
                                                     .map(tuple -> (String)tuple.getValueByField(FIELD_NAME))
                                                     .collect(Collectors.toList());
        List<Pair<String, Integer>> topNList = getTopN(collectedFields);

        LOG.info("Top {} positive words out of number of words in window interval: {}", TOP_N, tuplesInWindow.size());

        // Print results to the log and emit to the next bolt
        topNList.stream().forEach(pair-> {
            LOG.info(pair.getLeft()+LOG_DELIMITER+pair.getRight());
            collector.emit(new Values(System.currentTimeMillis(), pair.getLeft(), pair.getRight()));
        });
    }

    /**
     * Build top N list
     *
     * @param collectedFields
     * @return
     */
    private List<Pair<String, Integer>> getTopN(List<String> collectedFields) {
        List<Pair<String, Integer>> wordTuple = new ArrayList<>();

        Multiset<String> multiset = HashMultiset.create(collectedFields);

        multiset.entrySet().stream().forEach(s-> {
            wordTuple.add(Pair.of(s.getElement(), s.getCount()));
        });

        Collections.sort(wordTuple, new PairComparator());

        List<Pair<String, Integer>> top10List;
        if (wordTuple.size()<TOP_N){
            top10List = wordTuple;
        } else {
            top10List = wordTuple.subList(0, TOP_N);
        }
        return top10List;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "word", "frequency"));
    }

    private static class PairComparator implements Comparator<Pair<String, Integer>> {
        @Override
        public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
            return o2.getRight().compareTo(o1.getRight());
        }
    }
}
