package com.rain.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2. 读取数据： socket
        DataStreamSource<String> socketDS = executionEnvironment.socketTextStream("localhost", 7777);

        // TODO 3. 处理数据: 切换、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
//                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                .keyBy(value -> value.f0)
                .sum(1);

        // TODO 4. 输出
        sum.print();

        // TODO 5. 执行
        executionEnvironment.execute();
    }
}
