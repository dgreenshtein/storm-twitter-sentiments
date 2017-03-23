package com.davidgreenshtein.storm.twitter;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class ComparatorConceptualTest {

    @Test
    public void comtareTest(){


        List<Pair<String, Integer>> wordTuple = new ArrayList<>();

        wordTuple.add(Pair.of("bla bla", 2));
        wordTuple.add(Pair.of("bla bla22", 1));
        wordTuple.add(Pair.of("bla bla33", 3));

        wordTuple.add(Pair.of("bla bla100", 1));
        wordTuple.add(Pair.of("bla bla1002", 50));

        Collections.sort(wordTuple, (o1, o2) -> o2.getRight().compareTo(o1.getRight()));

        List<Pair<String, Integer>> top10List = null;
        if (wordTuple.size()<10){
            top10List = wordTuple;
        } else {
            top10List = wordTuple.subList(0, 10);
        }

        top10List.stream().forEach(s-> {
            System.out.println(s.getLeft()+"|"+s.getRight());
        });
    }
}
