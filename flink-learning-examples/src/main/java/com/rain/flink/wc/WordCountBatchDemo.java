package com.rain.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // TODO 2. 读取数据：从文件中读取
        DataSource<String> stringDataSource = executionEnvironment.readTextFile("word.txt");

        // TODO 3. 切分，转换 （word, 1）
        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // TODO 3.1 按照 空格 切分单词
                String[] words = s.split(" ");
                // TODO 3.2 将单词转换为 （word, 1）
                for (String word : words) {
                    Tuple2<String, Integer> of = Tuple2.of(word, 1);
                    // TODO 3.3 使用 Collector 向下法
                    collector.collect(of);
                }
            }
        });

        // TODO 4. 按照 word 分组
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = stringTuple2FlatMapOperator.groupBy(0);

        // TODO 5. 各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);

        // TODO 6. 输出
        sum.print();
    }
}
