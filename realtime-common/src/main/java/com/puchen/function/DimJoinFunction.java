package com.puchen.function;

import com.alibaba.fastjson.JSONObject;
import com.puchen.bean.TradeSkuOrderBean;

public interface DimJoinFunction<T> {
    String getId(T input);

    String getTableName();

    void join(T input, JSONObject jsonObject);
}
