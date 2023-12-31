package com.rain.flink.window;

import com.rain.flink.bean.WaterSensor;
import com.rain.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 老写法
//        sensorWS
//                .apply(
//                        new WindowFunction<WaterSensor, String, String, TimeWindow>() {
//                            /**
//                             *
//                             * @param s  分组的key
//                             * @param window 窗口对象
//                             * @param input 存的数据
//                             * @param out   采集器
//                             * @throws Exception
//                             */
//                            @Override
//                            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out) throws Exception {
//
//                            }
//                        }
//                )

        SingleOutputStreamOperator<String> process = sensorWS
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            /**
                             * 全窗口函数计算逻辑：  窗口触发时才会调用一次，统一计算窗口的所有数据
                             * @param s   分组的key
                             * @param context  上下文
                             * @param elements 存的数据
                             * @param out      采集器
                             * @throws Exception
                             */
                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                // 上下文可以拿到window对象，还有其他东西：侧输出流 等等
                                long startTs = context.window().getStart();
                                long endTs = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                                long count = elements.spliterator().estimateSize();

                                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                            }
                        }
                );

        process.print();

        env.execute();
    }
}

/*
*
* 输入
*
*
s1,1,1
s1,2,2
s1,3,3
s1,4,4
s1,5,5
s1,6,6
s1,7,7
*
*
* 输出
*
*
key=s1的窗口[2023-10-27 10:49:30.000,2023-10-27 10:49:40.000)包含5条数据===>[WaterSensor{id='s1', ts=1, vc=1}, WaterSensor{id='s1', ts=2, vc=2}, WaterSensor{id='s1', ts=3, vc=3}, WaterSensor{id='s1', ts=4, vc=4}, WaterSensor{id='s1', ts=5, vc=5}]
key=s1的窗口[2023-10-27 10:49:40.000,2023-10-27 10:49:50.000)包含2条数据===>[WaterSensor{id='s1', ts=6, vc=6}, WaterSensor{id='s1', ts=7, vc=7}]
*
* */
