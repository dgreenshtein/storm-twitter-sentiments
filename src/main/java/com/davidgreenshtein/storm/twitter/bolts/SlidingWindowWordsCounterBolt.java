package com.davidgreenshtein.storm.twitter.bolts;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class SlidingWindowWordsCounterBolt extends BaseWindowedBolt {

    public static String FIELD_NAME = "word";
    private static String LOG_DELIMITER = "|";

    private OutputCollector collector;
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowWordsCounterBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {

        List<String> collectedFields = new ArrayList<>();

        List<Tuple> tuplesInWindow = tupleWindow.get();

        // extract words in the current window
        for (Tuple tuple:tuplesInWindow) {
            collectedFields.add((String)tuple.getValueByField(FIELD_NAME));
        }
        List<Pair<String, Integer>> top10List = getTopN(collectedFields);

        LOG.info("--->Top 10 positive words out of number of words in window interval: " + tuplesInWindow.size());

        // Print results to the log and emit to the next bolt
        for (Pair<String, Integer> pair:top10List) {
            LOG.info(pair.getLeft()+LOG_DELIMITER+pair.getRight());
            collector.emit(new Values(System.currentTimeMillis(), pair.getLeft(), pair.getRight()));
        }
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

        for (Multiset.Entry s:multiset.entrySet()) {
            wordTuple.add(Pair.of((String)s.getElement(), s.getCount()));
        }

        Collections.sort(wordTuple, new PairComparator());

        List<Pair<String, Integer>> top10List = null;
        if (wordTuple.size()<10){
            top10List = wordTuple;
        } else {
            top10List = wordTuple.subList(0, 10);
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
