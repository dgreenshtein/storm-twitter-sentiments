package com.davidgreenshtein.storm.twitter;

import com.davidgreenshtein.storm.twitter.bolts.SentimentDiscoveryBolt;
import com.davidgreenshtein.storm.twitter.helper.MockTupleHelper;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
@RunWith(MockitoJUnitRunner.class)
public class SentimentsDiscoveryBoltTest {

    private static final String TEST_COMPONENT_ID = "comp_id";
    private static final String TEST_STREAM_ID = "stream_id";

    private SentimentDiscoveryBolt bolt;
    private BasicOutputCollector collector;

    @Before
    public void before(){
        this.collector = mock(BasicOutputCollector.class);
        this.bolt = new SentimentDiscoveryBolt();
        TopologyContext context = mock(TopologyContext.class);
        Map conf = mock(Map.class);
        this.bolt.prepare(conf, context);
    }

    @Test
    public void testPositive(){
        //given
        String sentence = "test very positive tweet";
        Tuple tuple = MockTupleHelper.mockTuple(TEST_COMPONENT_ID, TEST_STREAM_ID, SentimentDiscoveryBolt.FIELD_NAME, sentence);

        //when
        bolt.execute(tuple, collector);

        //then
        verify(collector).emit(new Values(sentence));
    }

    @Test
    public void testNotPositive(){
        //given
        String sentence = "test very bad tweet";
        Tuple tuple = MockTupleHelper.mockTuple(TEST_COMPONENT_ID, TEST_STREAM_ID, SentimentDiscoveryBolt.FIELD_NAME, sentence);

        //when
        bolt.execute(tuple, collector);

        //then
        verifyZeroInteractions(collector);
    }

    @Test
    public void testNull(){
        //given
        String sentence = null;
        Tuple tuple = MockTupleHelper.mockTuple(TEST_COMPONENT_ID, TEST_STREAM_ID, SentimentDiscoveryBolt.FIELD_NAME, sentence);

        //when
        bolt.execute(tuple, collector);

        //then
        verifyZeroInteractions(collector);
    }

}
