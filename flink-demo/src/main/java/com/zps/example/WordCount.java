package com.zps.example;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * Created by zps on 2020/9/6 17:18
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从本地文件读取
        DataSource<String> dataSource = env.readTextFile("D:\\ideaspacework\\Flink\\flink-demo\\src\\main\\resources\\word.txt");

        AggregateOperator<Tuple2<String, Integer>> operator = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                // 将文本分割
                String[] strings = value.toLowerCase().split(" ");
                for (String word : strings) {
                    if (word.length() > 0) {
                        out.collect(new Tuple2<String, Integer>(word, 1));
                    }
                }
            }
        }).groupBy(0).sum(1);

        operator.print();
    }
}
