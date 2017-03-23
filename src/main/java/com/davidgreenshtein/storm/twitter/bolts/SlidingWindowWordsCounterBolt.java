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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class SlidingWindowWordsCounterBolt extends BaseWindowedBolt {

    private OutputCollector collector;
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowWordsCounterBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tuplesInWindow = tupleWindow.get();
        List<String> collectedFields = tuplesInWindow.stream()
                                            .map(tuple -> (String)tuple.getValueByField("word"))
                                            .collect(Collectors.toList());

        Multiset<String> multiset = HashMultiset.create(collectedFields);

        List<Pair<String, Integer>> wordTuple = new ArrayList<>();
        multiset.entrySet().stream().forEach(s-> {
            wordTuple.add(Pair.of(s.getElement(), s.getCount()));
        });


        Collections.sort(wordTuple, (o1, o2) -> o2.getRight().compareTo(o1.getRight()));

        List<Pair<String, Integer>> top10List = null;
        if (wordTuple.size()<10){
            top10List = wordTuple;
        } else {
            top10List = wordTuple.subList(0, 10);
        }

        LOG.info("--->Top 10 positive words out of number of words in window interval: " + tuplesInWindow.size());

        top10List.stream().forEach(s-> {
            LOG.info(s.getLeft()+"|"+s.getRight());
            collector.emit(new Values(Instant.now().getEpochSecond(), s.getLeft(), s.getRight()));
        });

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "word", "frequency"));
    }
}
