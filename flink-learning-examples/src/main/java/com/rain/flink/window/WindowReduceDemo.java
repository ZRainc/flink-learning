package com.rain.flink.window;

import com.rain.flink.bean.WaterSensor;
import com.rain.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 2. 窗口函数： 增量聚合 Reduce
        /**
         * 窗口的reduce：
         * 1、相同key的第一条数据来的时候，不会调用reduce方法
         * 2、增量聚合： 来一条数据，就会计算一次，但是不会输出
         * 3、在窗口触发的时候，才会输出窗口的最终计算结果
         */
        SingleOutputStreamOperator<WaterSensor> reduce = sensorWS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("调用reduce方法，value1=" + value1 + ",value2=" + value2);
                        return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                    }
                }
        );

        reduce.print();

        env.execute();
    }
}
/*
*
* 输入
*
s1,1,1
s1,2,2
s1,3,3
s1,4,4
s1,5,5
s1,6,6
*
*
* 输出
*
*
调用reduce方法，value1=WaterSensor{id='s1', ts=1, vc=1},value2=WaterSensor{id='s1', ts=2, vc=2}
调用reduce方法，value1=WaterSensor{id='s1', ts=2, vc=3},value2=WaterSensor{id='s1', ts=3, vc=3}
WaterSensor{id='s1', ts=3, vc=6}
调用reduce方法，value1=WaterSensor{id='s1', ts=4, vc=4},value2=WaterSensor{id='s1', ts=5, vc=5}
调用reduce方法，value1=WaterSensor{id='s1', ts=5, vc=9},value2=WaterSensor{id='s1', ts=6, vc=6}
WaterSensor{id='s1', ts=6, vc=15}
*
*
* */


