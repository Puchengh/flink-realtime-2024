package com.puchen.realtime.dws.function;

import com.puchen.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<keyword STRING, length INT>"))
public class KwSplit extends TableFunction<Row> {

    public void eval(String keywords) {

        List<String> strings = IkUtil.IKSplit(keywords);
        for (String s : strings) {
            collect(Row.of(s, s.length()));
        }
    }
}
