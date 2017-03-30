package com.davidgreenshtein.storm.twitter;

import com.davidgreenshtein.storm.twitter.bolts.WordSplitterBolt;
import com.davidgreenshtein.storm.twitter.helper.MockTupleHelper;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
@RunWith(MockitoJUnitRunner.class)
public class WordSplitterBoltTest {

    private static final String TEST_COMPONENT_ID = "comp_id";
    private static final String TEST_STREAM_ID = "stream_id";
    private static final String TUPLE_FIELD_ID = "sentence";

    private WordSplitterBolt bolt;

    @Mock
    private BasicOutputCollector collector;

    @Before
    public void before(){
        this.bolt = new WordSplitterBolt(2);
    }

    @Test
    public void passWordNotInTheIgnoreList(){
        //given
        String sentence = "test sentence";
        String expectedWord1 = "test";
        String expectedWord2 = "sentence";
        Tuple tuple = MockTupleHelper.mockTuple(TEST_COMPONENT_ID, TEST_STREAM_ID, TUPLE_FIELD_ID, sentence);

        //when
        bolt.execute(tuple, collector);

        //then
        verify(collector).emit(new Values(expectedWord1));
        verify(collector).emit(new Values(expectedWord2));
    }
}
