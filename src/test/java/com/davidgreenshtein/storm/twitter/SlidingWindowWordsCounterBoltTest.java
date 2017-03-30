package com.davidgreenshtein.storm.twitter;

import com.davidgreenshtein.storm.twitter.bolts.SlidingWindowWordsCounterBolt;
import com.davidgreenshtein.storm.twitter.helper.MockTupleHelper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.util.Map;
import java.util.Objects;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Created by davidgreenshtein on 30.03.17.
 */
public class SlidingWindowWordsCounterBoltTest {

    private static final String TEST_COMPONENT_ID = "comp_id";
    private static final String TEST_STREAM_ID = "stream_id";
    private static final int WORD_POSITION = 1;
    private static final int COUNTER_POSITION = 2;

    private BaseWindowedBolt bolt;
    private OutputCollector collector;

    @Before
    public void before(){
        this.bolt = new SlidingWindowWordsCounterBolt().withWindow(new BaseWindowedBolt.Count(4), new BaseWindowedBolt.Count(2));
        Map conf = mock(Map.class);
        this.collector = mock(OutputCollector.class);
        TopologyContext context = mock(TopologyContext.class);
        bolt.prepare(conf, context, collector);
    }

    @Test
    public void testValidWindow(){

        //given
        String [] expectedWords = new String[]{"blah", "hey", "yo", "hey"};

        TupleWindow tupleWindow = MockTupleHelper.mockTupleWindow(TEST_COMPONENT_ID, TEST_STREAM_ID, SlidingWindowWordsCounterBolt.FIELD_NAME, expectedWords);

        //when
        bolt.execute(tupleWindow);

        //then
        verify(collector).emit(argThat(new ValuesArgumentMatcher("hey", 2)));
        verify(collector).emit(argThat(new ValuesArgumentMatcher("yo", 1)));
        verify(collector).emit(argThat(new ValuesArgumentMatcher("blah", 1)));
    }

    @Test
    public void testEmptyWindow() {

        //given
        String[] expectedWords = new String[]{};

        TupleWindow tupleWindow = MockTupleHelper.mockTupleWindow(TEST_COMPONENT_ID, TEST_STREAM_ID, SlidingWindowWordsCounterBolt.FIELD_NAME, expectedWords);

        //when
        bolt.execute(tupleWindow);

        verifyZeroInteractions(collector);
    }

    private class ValuesArgumentMatcher extends ArgumentMatcher<Values> {

        String word;
        int count;

        public ValuesArgumentMatcher(String word, int count){
            this.word = word;
            this.count = count;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof Values){
                Values v = (Values) o;
                if (Objects.equals(this.word, v.get(WORD_POSITION)) && Objects.equals(v.get(COUNTER_POSITION), this.count)) return true;
            }
            return false;
        }
    }
}
