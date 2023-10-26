package com.rain.flink.functions;

import com.rain.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
