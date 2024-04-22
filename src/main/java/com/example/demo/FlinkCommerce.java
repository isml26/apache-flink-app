package com.example.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCommerce {
    public static void main(String[] args) throws Exception {
        /*
        Setup execution environment, which is main entrypoint
        to building Flink apps;
         */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.execute("Flink Java API");
    }
}
