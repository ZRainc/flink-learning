package com.rain.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // IDEA 运行时，也可以看到webUI, 一般用于本地测试，需要引入一个jar包 flink-runtime-web
        // 在 IDEA 运行，不指定并行度，默认就是 电脑的 线程数
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 对全局生效
        executionEnvironment.setParallelism(3);

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
//                .setParallelism(2)
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

/*
*
*  并行度的优先级：
*   代码：算子 > 代码：env > 提交时指定 > 配置文件
*
* */
