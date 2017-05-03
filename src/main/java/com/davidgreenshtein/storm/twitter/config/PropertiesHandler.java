package com.davidgreenshtein.storm.twitter.config;


/**
 * Created by davidgreenshtein on 30.03.17.
 */
public class PropertiesHandler {

    public static final String TWITTER_CONSUMER_KEY = "twitter.consumer.key";
    public static final String TWITTER_CONSUMER_SECRET = "twitter.consumer.secret";
    public static final String TWITTER_CONSUMER_TOKEN = "twitter.access.token";
    public static final String TWITTER_CONSUMER_TOKEN_SECRET = "twitter.access.token.secret";
    public static final String TOP_N_NUMBER = "top.n.number";
    public static final String TWITTER_SPOUT_HINT = "twitter.spout.parallelizm.hint";
    public static final String SENTIMENTS_DISCOVERY_HINT = "sentiment.discovery.bolt.parallelizm.hint";
    public static final String WORD_SPLITTER_HINT = "word.splitter.bolt.parallelizm.hint";
    public static final String WORD_SPLITTER_MINIMUM_WORD_LENGTH = "word.splitter.bolt.minimum.word.lenth";
    public static final String IGNORE_WORDS_HINT = "ignore.words.parallelizm.hint";
    public static final String SLIDING_WINDOW_LENGTH = "sliding.window.bolt.window.length";
    public static final String SLIDING_WINDOW_INTERVAL = "sliding.window.bolt.window.interval";
    public static final String SLIDING_WNDOW_HINT = "sliding.window.bolt.parallelizm.hint";
    public static final String HDFS_HINT = "hdfs.bolt.parallelizm.hint";
    public static final String NUM_WORKERS = "storm.num.workers";

    private final java.util.Properties props;

    public PropertiesHandler(java.util.Properties props){
        this.props = props;
    }

    public Integer getInteger(String name){
        return Integer.valueOf(this.props.getProperty(name));
    }

    public String getString(String name){
        return this.props.getProperty(name);
    }
}
