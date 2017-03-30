package com.davidgreenshtein.storm.twitter.helper;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by davidgreenshtein on 30.03.17.
 */
public class MockTupleHelper {

    public static Tuple mockTuple(String componentId, String streamId, String fieldName, String word) {
        Tuple tuple = mockTuple(componentId, streamId);
        Mockito.when(tuple.getValueByField(fieldName)).thenReturn(word);
        return tuple;
    }

    public static Tuple mockTickTuple() {
        return mockTuple("__system", "__tick");
    }

    private static Tuple mockTuple(String componentId, String streamId) {
        Tuple tuple = Mockito.mock(Tuple.class);
        Mockito.when(tuple.getSourceComponent()).thenReturn(componentId);
        Mockito.when(tuple.getSourceStreamId()).thenReturn(streamId);
        return tuple;
    }

    public static TupleWindow mockTupleWindow(String componentId, String streamId, String fieldName, String [] words) {
        TupleWindow tupleWindow = Mockito.mock(TupleWindow.class);
        List<Tuple> listOfTupples = new ArrayList<>();
        for (String word : words) {
            Tuple tuple = mockTuple(componentId, streamId, fieldName, word);
            listOfTupples.add(tuple);
        }

        Mockito.when(tupleWindow.get()).thenReturn(listOfTupples);
        return tupleWindow;
    }
}
