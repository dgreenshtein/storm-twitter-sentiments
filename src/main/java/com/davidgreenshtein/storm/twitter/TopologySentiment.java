package com.davidgreenshtein.storm.twitter;

import com.davidgreenshtein.storm.twitter.bolts.IgnoreWordsBolt;
import com.davidgreenshtein.storm.twitter.bolts.SentimentDiscoveryBolt;
import com.davidgreenshtein.storm.twitter.bolts.SlidingWindowWordsCounterBolt;
import com.davidgreenshtein.storm.twitter.bolts.WordSplitterBolt;
import com.davidgreenshtein.storm.twitter.config.PropertiesNames;
import com.davidgreenshtein.storm.twitter.spout.TwitterSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.AbstractHdfsBolt;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;
/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class TopologySentiment {

    private static final String TOPOLOGY_NAME = "storm-twitter-sentiment-words";
    private static final String HDFS_BOLT_PROPERTIES = "hdfs.bolt.properties";
    private static final Logger LOG = LoggerFactory.getLogger(TopologySentiment.class);

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, IOException {

        if (args.length != 5){
            System.out.println("Expected 5 parameters: <fsUrl>, <fsOutputPath>, <hadoopUser>, <local>, <config file path>");
            return;
        }
        String fsUrl = args[0];
        String fsOutputPath = args[1];
        String hadoopUser = args[2];

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("TwitterSpout", new TwitterSpout(), 1);
        builder.setBolt("SentimentDiscoveryBolt", new SentimentDiscoveryBolt(), 5).shuffleGrouping("TwitterSpout");
        builder.setBolt("WordSplitterBolt", new WordSplitterBolt(4),1).shuffleGrouping("SentimentDiscoveryBolt");
        builder.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt(),1).shuffleGrouping("WordSplitterBolt");
        builder.setBolt("SlidingWindowWordsCounterBolt", new SlidingWindowWordsCounterBolt().withWindow(new Count(400), new Count(50)), 1)
               .shuffleGrouping("IgnoreWordsBolt");
        builder.setBolt("HdfsBolt", initHdfBolt(fsUrl, fsOutputPath, hadoopUser),1).shuffleGrouping("SlidingWindowWordsCounterBolt");

        if ("local".equals(args[3])){
            final LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, prepareConfig(args[4]), builder.createTopology());
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    cluster.killTopology(TOPOLOGY_NAME);
                    cluster.shutdown();
                }
            });
        } else {
            StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME,prepareConfig(args[4]), builder.createTopology());
        }
    }

    private static Map prepareConfig(String configFilePath) throws IOException{

        Config config = new Config();
        Properties prop = new Properties();
        InputStream input = TopologySentiment.class.getClassLoader().getResourceAsStream(configFilePath);

        if(input==null){
            LOG.error("Enable to read properties file from path:{}", configFilePath);
        } else {
            prop.load(input);
            config.put(PropertiesNames.TWITTER_CONSUMER_KEY, prop.getProperty(PropertiesNames.TWITTER_CONSUMER_KEY));
            config.put(PropertiesNames.TWITTER_CONSUMER_SECRET, prop.getProperty(PropertiesNames.TWITTER_CONSUMER_SECRET));
            config.put(PropertiesNames.TWITTER_CONSUMER_TOKEN, prop.getProperty(PropertiesNames.TWITTER_CONSUMER_TOKEN));
            config.put(PropertiesNames.TWITTER_CONSUMER_TOKEN_SECRET, prop.getProperty(PropertiesNames.TWITTER_CONSUMER_TOKEN_SECRET));
        }

        config.setMessageTimeoutSecs(120);
        config.setNumWorkers(3);
        Map<String, String>  hdfsBoltConfigs = new HashMap<>();
        hdfsBoltConfigs.put("dfs.client.use.datanode.hostname", "true");
        config.put(HDFS_BOLT_PROPERTIES, hdfsBoltConfigs);
        return config;
    }

    private static AbstractHdfsBolt initHdfBolt(String fsUrl, String fsOutputPath, String hadoopUser){

        System.setProperty("HADOOP_USER_NAME", hadoopUser);

        // sync the filesystem after every 200 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(200);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.KB);

        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withExtension(".plain")
                .withPath(fsOutputPath);

        return new HdfsBolt()
                .withFsUrl(fsUrl)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .withConfigKey(HDFS_BOLT_PROPERTIES);
    }

}
