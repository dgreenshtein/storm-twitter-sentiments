# Twitter sentiments real time analyzer
[![Build Status](https://travis-ci.org/dgreenshtein/storm-twitter-sentiments.svg?branch=master)](https://travis-ci.org/dgreenshtein/storm-twitter-sentiments)
[![codecov](https://codecov.io/gh/dgreenshtein/storm-twitter-sentiments/branch/master/graph/badge.svg)](https://codecov.io/gh/dgreenshtein/storm-twitter-sentiments)

Positive tweets words trending with Apache Storm

This project is a prototype integrating Twitter with Storm and HDFS

Storm topology business logic
* analyze in real-time stream of tweets written in English
* find positive tweets
* define words trending in predefined window interval
* writing results to HDFS

For tweets sentimental analysis I used https://nlp.stanford.edu/software/ open source library

# How to run with maven
```
mvn exec:java -Dexec.mainClass="com.davidgreenshtein.storm.twitter.TopologySentiment"
              -Dtwitter4j.oauth.consumerKey=*** 
              -Dtwitter4j.oauth.consumerSecret=***
              -Dtwitter4j.oauth.accessToken=***
              -Dtwitter4j.oauth.accessTokenSecret=***
              -Dexec.args="hdfs://[namenode host name]:8020 [hdfs output path] [hdfs user name] local"
```

# How to run on cluster
```
storm jar twitter-sentiments-1.0-SNAPSHOT.jar \
 com.davidgreenshtein.storm.twitter.TopologySentiment \
 hdfs://[namenode host name]:8020 \
 [hdfs output path] \
 [hdfs user name] \
 distributed \
 [properties file location path]
```
