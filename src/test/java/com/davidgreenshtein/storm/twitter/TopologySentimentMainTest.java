package com.davidgreenshtein.storm.twitter;

import com.davidgreenshtein.storm.twitter.config.PropertiesHandler;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.*;

import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.Map;

/**
 * Created by davidgreenshtein on 03.05.17.
 */
public class TopologySentimentMainTest {

    TopologySentiment topologySentiment;

    @Before
    public void before(){
        this.topologySentiment = new TopologySentiment();
        PropertiesHandler propertiesHandler = mock(PropertiesHandler.class);
        when(propertiesHandler.getInteger(any(String.class))).thenReturn(1);
        when(propertiesHandler.getString(any(String.class))).thenReturn("test string");
        topologySentiment.setPropertiesHandler(propertiesHandler);
    }

    @Test
    public void testInitTopology() throws IOException {
        topologySentiment.init(new String[]{"host", "path", "gap", "local", "config"});
        TopologyBuilder builder = topologySentiment.getBuilder();
        assertNotNull(builder);
    }

    @Test
    public void testPrepareTopologyConfig() throws Exception {
        int numWorkers = 3;
        Map result = Whitebox.invokeMethod(this.topologySentiment, "prepareTopologyConfig", numWorkers);
        assertEquals(numWorkers, result.get("topology.workers"));
    }

    @Test
    public void testinitProperties() throws Exception {
        String fileName = "config.properties";
        Whitebox.invokeMethod(this.topologySentiment, "initProperties", fileName);
        assertNotNull(topologySentiment.getPropertiesHandler());
    }

}
