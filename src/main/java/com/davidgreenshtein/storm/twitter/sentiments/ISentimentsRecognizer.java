package com.davidgreenshtein.storm.twitter.sentiments;

/**
 * Created by davidgreenshtein on 30.03.17.
 */
public interface ISentimentsRecognizer {

    String discoverSentiment(String line);
}
