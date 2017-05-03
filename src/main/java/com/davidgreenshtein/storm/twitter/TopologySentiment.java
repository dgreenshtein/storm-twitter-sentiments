package com.davidgreenshtein.storm.twitter;

import com.davidgreenshtein.storm.twitter.bolts.IgnoreWordsBolt;
import com.davidgreenshtein.storm.twitter.bolts.SentimentDiscoveryBolt;
import com.davidgreenshtein.storm.twitter.bolts.SlidingWindowWordsCounterBolt;
import com.davidgreenshtein.storm.twitter.bolts.WordSplitterBolt;
import com.davidgreenshtein.storm.twitter.config.PropertiesHandler;
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class TopologySentiment implements Closeable {

    private static final String TOPOLOGY_NAME = "storm-twitter-sentiment-words";
    private static final String HDFS_BOLT_PROPERTIES = "hdfs.bolt.properties";
    private static final int FS_URL = 0;
    private static final int FS_OUTPUT_PATH = 1;
    private static final int HADOOP_USER = 2;
    private static final int CONFIG_PATH = 4;
    private static final int IS_LOCAL_MODE = 3;

    private PropertiesHandler propertiesHandler;
    private TopologyBuilder builder;

    private static final Logger LOG = LoggerFactory.getLogger(TopologySentiment.class);

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, IOException {

        try (TopologySentiment job = new TopologySentiment()) {
            job.init(args);
            if ("local".equals(args[IS_LOCAL_MODE])) {
                job.runLocal();
            } else {
                job.run();
            }
        }
    }

    public void init(String[] args) throws IOException {
        if (args.length != 5) {
            System.out.println("Expected 5 parameters: <fsUrl>, <fsOutputPath>, <hadoopUser>, <local>, <config file path>");
            return;
        }
        String fsUrl = args[FS_URL];
        String fsOutputPath = args[FS_OUTPUT_PATH];
        String hadoopUser = args[HADOOP_USER];

        initProperties(args[CONFIG_PATH]);
        this.builder = new TopologyBuilder();
        builder.setSpout("TwitterSpout", new TwitterSpout(), this.propertiesHandler.getInteger(PropertiesHandler.TWITTER_SPOUT_HINT));
        builder.setBolt("SentimentDiscoveryBolt",
                        new SentimentDiscoveryBolt(),
                        this.propertiesHandler.getInteger(PropertiesHandler.SENTIMENTS_DISCOVERY_HINT))
               .shuffleGrouping("TwitterSpout");
        builder.setBolt("WordSplitterBolt",
                        new WordSplitterBolt(this.propertiesHandler.getInteger(PropertiesHandler.WORD_SPLITTER_MINIMUM_WORD_LENGTH)),
                        this.propertiesHandler.getInteger(PropertiesHandler.WORD_SPLITTER_HINT))
               .shuffleGrouping("SentimentDiscoveryBolt");
        builder.setBolt("IgnoreWordsBolt",
                        new IgnoreWordsBolt(),
                        this.propertiesHandler.getInteger(PropertiesHandler.IGNORE_WORDS_HINT))
               .shuffleGrouping("WordSplitterBolt");
        builder.setBolt("SlidingWindowWordsCounterBolt",
                        new SlidingWindowWordsCounterBolt()
                                .withWindow(new Count(this.propertiesHandler.getInteger(PropertiesHandler.SLIDING_WINDOW_LENGTH)),
                                            new Count(this.propertiesHandler.getInteger(PropertiesHandler.SLIDING_WINDOW_INTERVAL))),
                        this.propertiesHandler.getInteger(PropertiesHandler.SLIDING_WNDOW_HINT))
               .shuffleGrouping("IgnoreWordsBolt");
        builder.setBolt("HdfsBolt",
                        initHdfsBolt(fsUrl, fsOutputPath, hadoopUser),
                        this.propertiesHandler.getInteger(PropertiesHandler.HDFS_HINT))
               .shuffleGrouping("SlidingWindowWordsCounterBolt");
    }

    private void runLocal() {
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, prepareTopologyConfig(this.propertiesHandler.getInteger(PropertiesHandler.NUM_WORKERS)), builder.createTopology
                ());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }));
        LOG.info("Storm topology started in Local mode");
    }

    private void run() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, prepareTopologyConfig(this.propertiesHandler.getInteger(PropertiesHandler.NUM_WORKERS)), builder.createTopology());
        LOG.info("Storm topology started in Cluster mode");
    }

    private void initProperties(String configFilePath) throws IOException {
        InputStream input = TopologySentiment.class.getClassLoader().getResourceAsStream(configFilePath);
        if (input == null) {
            LOG.error("Enable to read properties file from path:{}", configFilePath);
        } else {
            Properties props = new java.util.Properties();
            props.load(input);
            this.propertiesHandler = new PropertiesHandler(props);
        }
    }

    private Map prepareTopologyConfig(Integer numWorkers) {

        Config config = new Config();
        config.put(PropertiesHandler.TWITTER_CONSUMER_KEY, this.propertiesHandler.getString(PropertiesHandler.TWITTER_CONSUMER_KEY));
        config.put(PropertiesHandler.TWITTER_CONSUMER_SECRET, this.propertiesHandler.getString(PropertiesHandler.TWITTER_CONSUMER_SECRET));
        config.put(PropertiesHandler.TWITTER_CONSUMER_TOKEN, this.propertiesHandler.getString(PropertiesHandler.TWITTER_CONSUMER_TOKEN));
        config.put(PropertiesHandler.TWITTER_CONSUMER_TOKEN_SECRET, this.propertiesHandler.getString(PropertiesHandler
                                                                                                             .TWITTER_CONSUMER_TOKEN_SECRET));
        config.put(PropertiesHandler.TOP_N_NUMBER, this.propertiesHandler.getString(PropertiesHandler.TOP_N_NUMBER));

        config.setMessageTimeoutSecs(120);
        config.setNumWorkers(numWorkers);
        Map<String, String> hdfsBoltConfigs = new HashMap<>();
        hdfsBoltConfigs.put("dfs.client.use.datanode.hostname", "true");
        config.put(HDFS_BOLT_PROPERTIES, hdfsBoltConfigs);
        return config;
    }

    private static AbstractHdfsBolt initHdfsBolt(String fsUrl, String fsOutputPath, String hadoopUser) {

        System.setProperty("HADOOP_USER_NAME", hadoopUser);

        // sync the filesystem after every 200 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(200);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(250.0f, FileSizeRotationPolicy.Units.MB);

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

    @Override
    public void close() throws IOException {

    }

    public TopologyBuilder getBuilder() {
        return builder;
    }

    public void setPropertiesHandler(PropertiesHandler propertiesHandler) {
        this.propertiesHandler = propertiesHandler;
    }


    public PropertiesHandler getPropertiesHandler() {
        return propertiesHandler;
    }
}
