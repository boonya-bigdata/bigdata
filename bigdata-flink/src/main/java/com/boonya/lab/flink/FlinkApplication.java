package com.boonya.lab.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Application Entry Point
 */
public class FlinkApplication {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkApplication.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Application...");

        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure environment
        env.setParallelism(1);

        // TODO: Add your Flink job logic here

        // Execute the job
        env.execute("Flink Application");

        LOG.info("Flink Application completed.");
    }
}
