package com.boonya.lab.flink.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Example: Word Count using Flink DataStream API
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception {
        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create data stream from socket or collection
        DataStream<String> textStream = env.fromElements(
                "Hello World",
                "Hello Flink",
                "Hello Big Data",
                "World of Flink"
        );

        // Perform word count
        DataStream<Tuple2<String, Integer>> wordCounts = textStream
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        // Print results
        wordCounts.print();

        // Execute the program
        env.execute("Word Count Example");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined FlatMapFunction.
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // Emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(Tuple2.of(token, 1));
                }
            }
        }
    }
}