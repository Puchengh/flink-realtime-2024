package com.puchen.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.puchen.bean.TrafficPageViewBean;
import org.apache.flink.api.common.functions.MapFunction;

public class  DorisMapFunction<T> implements MapFunction<T,String> {
    @Override
    public String map(T trafficPageViewBean) throws Exception {
        SerializeConfig serializeConfig = new SerializeConfig();
        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSONObject.toJSONString(trafficPageViewBean, serializeConfig);
    }
}
