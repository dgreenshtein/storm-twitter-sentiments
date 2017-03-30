package com.davidgreenshtein.storm.twitter.sentiments;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.util.Properties;

/**
 * Created by davidgreenshtein on 22.03.17.
 */
public class SentimentsRecognizer {

    private final StanfordCoreNLP tokenizer;
    private final StanfordCoreNLP pipeline;

    public SentimentsRecognizer(){
        Properties tokenizerProps = new Properties();
        Properties pipelineProps = new Properties();

        pipelineProps.setProperty("annotators", "parse, sentiment");
        pipelineProps.setProperty("parse.binaryTrees", "true");
        pipelineProps.setProperty("enforceRequirements", "false");
        tokenizerProps.setProperty("annotators", "tokenize, ssplit");

        tokenizer = new StanfordCoreNLP(tokenizerProps);
        pipeline = new StanfordCoreNLP(pipelineProps);
    }


    public String discoverSentiment(String line) {

        Annotation annotation = this.tokenizer.process(line);
        pipeline.annotate(annotation);
        if (annotation.get(CoreAnnotations.SentencesAnnotation.class).iterator().hasNext()) {
            CoreMap sentence = annotation.get(CoreAnnotations.SentencesAnnotation.class).iterator().next();
            return sentence.get(SentimentCoreAnnotations.SentimentClass.class);
        }
        return null;
    }
}
