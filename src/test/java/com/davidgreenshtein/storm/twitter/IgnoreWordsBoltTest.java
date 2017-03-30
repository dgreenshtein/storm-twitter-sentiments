package com.davidgreenshtein.storm.twitter;

import com.davidgreenshtein.storm.twitter.bolts.IgnoreWordsBolt;
import com.davidgreenshtein.storm.twitter.helper.MockTupleHelper;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class IgnoreWordsBoltTest {

    private static final String TEST_COMPONENT_ID = "comp_id";
    private static final String TEST_STREAM_ID = "stream_id";
    private static final String TUPLE_FIELD_ID = "word";

    private IgnoreWordsBolt bolt;
    private BasicOutputCollector collector;

    @Before
    public void before(){
        this.bolt = new IgnoreWordsBolt();
        this.collector = mock(BasicOutputCollector.class);
    }

    @Test
    public void passWordNotInTheIgnoreList(){
        //given
        String expectedWord = "test_word";

        Tuple tuple = MockTupleHelper.mockTuple(TEST_COMPONENT_ID, TEST_STREAM_ID, TUPLE_FIELD_ID, expectedWord);

        //when
        bolt.execute(tuple, collector);

        //then
        verify(collector).emit(new Values(expectedWord));
    }

    @Test
    public void filterWordInTheList(){
        //given
        String expectedWord = "from";
        Tuple tuple = MockTupleHelper.mockTuple(TEST_COMPONENT_ID, TEST_STREAM_ID, TUPLE_FIELD_ID, expectedWord);

        //when
        bolt.execute(tuple, collector);

        //then
        verifyZeroInteractions(collector);
    }

    @Test
    public void nullWordShouldSucceed(){
        //given
        String expectedWord = null;
        Tuple tuple = MockTupleHelper.mockTuple(TEST_COMPONENT_ID, TEST_STREAM_ID, TUPLE_FIELD_ID, expectedWord);

        //when
        bolt.execute(tuple, collector);

        //then
        verifyZeroInteractions(collector);
    }

}
