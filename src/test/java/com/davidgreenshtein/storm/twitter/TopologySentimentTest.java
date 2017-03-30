package com.davidgreenshtein.storm.twitter;

import com.davidgreenshtein.storm.twitter.bolts.IgnoreWordsBolt;
import com.davidgreenshtein.storm.twitter.bolts.SentimentDiscoveryBolt;
import com.davidgreenshtein.storm.twitter.bolts.WordSplitterBolt;
import com.davidgreenshtein.storm.twitter.helper.RandomSentenceTestSpout;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestJob;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Created by davidgreenshtein on 23.03.17.
 *
 * Integration test using test Spout
 */
public class TopologySentimentTest {

    @Test
    public void testBasicTopology() {

        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(1);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) {
                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("1", new RandomSentenceTestSpout(true), 2);
                builder.setBolt("2", new SentimentDiscoveryBolt(), 3).shuffleGrouping("1");
                builder.setBolt("3", new WordSplitterBolt(3), 1).shuffleGrouping("2");
                builder.setBolt("4", new IgnoreWordsBolt(), 1).shuffleGrouping("3");

                StormTopology topology = builder.createTopology();

                // complete the topology

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("1", new Values("today is just a nice day"), new Values("good day is better"),
                                          new Values("but very bad weather"));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(2);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology,
                                                      completeTopologyParam);

                // check whether the result is right
                assertTrue(Testing.multiseteq(new Values(
                                                    new Values("today is just a nice day"),
                                                    new Values("good day is better"),
                                                    new Values("but very bad weather")),
                                            Testing.readTuples(result, "1")));


                // expect only positive sentence
                assertTrue(Testing.multiseteq(new Values(
                                                    new Values("today is just a nice day"),
                                                    new Values("good day is better")),
                                              Testing.readTuples(result, "2")));

                // expect splited words having lenth bigger or equals to 3
                assertTrue(Testing.multiseteq(new Values(
                                                      new Values("today"),
                                                      new Values("just"),
                                                      new Values("nice"),
                                                      new Values("day"),
                                                      new Values("good"),
                                                      new Values("day"),
                                                      new Values("better")),
                                              Testing.readTuples(result, "3")));

                // exclude commonly used word "just" and "good"
                assertTrue(Testing.multiseteq(new Values(
                                                      new Values("today"),
                                                      new Values("nice"),
                                                      new Values("day"),
                                                      new Values("day"),
                                                      new Values("better")),
                                              Testing.readTuples(result, "4")));
            }

        });
    }

}

