package com.davidgreenshtein.storm.twitter.spout;

import com.davidgreenshtein.storm.twitter.config.PropertiesNames;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class TwitterSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        Configuration configuration = configurePermissions(conf);

        queue = new LinkedBlockingQueue<Status>(1000);
        this.collector = collector;

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
            }

            @Override
            public void onException(Exception e) {
            }
        };

        FilterQuery tweetFilterQuery = new FilterQuery();
        // take english tweets only
        tweetFilterQuery.language("en");

        TwitterStreamFactory factory = (configuration != null) ? new TwitterStreamFactory(configuration) : new TwitterStreamFactory();

        twitterStream = factory.getInstance();
        twitterStream.addListener(listener);
        twitterStream.filter(tweetFilterQuery);
        twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(ret.getText()));
        }
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    private Configuration configurePermissions(Map m){

        if(m.get(PropertiesNames.TWITTER_CONSUMER_KEY) != null
            && m.get(PropertiesNames.TWITTER_CONSUMER_SECRET) != null
                && m.get(PropertiesNames.TWITTER_CONSUMER_TOKEN) != null
                && m.get(PropertiesNames.TWITTER_CONSUMER_TOKEN_SECRET) != null){

            return new ConfigurationBuilder()
                    .setOAuthConsumerKey((String)m.get(PropertiesNames.TWITTER_CONSUMER_KEY))
                    .setOAuthConsumerSecret((String)m.get(PropertiesNames.TWITTER_CONSUMER_SECRET))
                    .setOAuthAccessToken((String)m.get(PropertiesNames.TWITTER_CONSUMER_TOKEN))
                    .setOAuthAccessTokenSecret((String)m.get(PropertiesNames.TWITTER_CONSUMER_TOKEN_SECRET))
                    .build();
        }

        return null;

    }
}
