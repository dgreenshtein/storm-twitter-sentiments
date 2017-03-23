# storm-twitter-sentiments
Positive tweets words trending with storm

This project is a prototype intergating Twitter with Storm and HDFS

Storm topology business logic
* analyze in real time stream of tweets written in English
* find positive tweets
* define words trending in predefinned window interval
* writing results to HDFS

For tweets sentimental analisys I used https://nlp.stanford.edu/software/ open source library

# How to run with maven
```
mvn exec:java -Dexec.mainClass="com.davidgreenshtein.storm.twitter.TopologySentiment"
              -Dtwitter4j.oauth.consumerKey=*** 
              -Dtwitter4j.oauth.consumerSecret=***
              -Dtwitter4j.oauth.accessToken=***
              -Dtwitter4j.oauth.accessTokenSecret=***
              -Dexec.args="hdfs://[namenode host name]:8020 [hdfs output path] [hdfs user name] local"
```
