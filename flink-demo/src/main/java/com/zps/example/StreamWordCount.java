package com.zps.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by zps on 2020/9/10 21:50
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //创建流式输入, 需要先启动nc 监听端口
        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 10086);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2<>(s, 1));
            }
        }).keyBy(0).sum(1);

        streamOperator.print();
        //批处理不需要执行此步骤

        env.execute("任务一");  //dev1

        System.out.println("AAA");

        System.out.println("AAA");

        System.out.println("AAA");

        System.out.println("AAA");
    }
}
