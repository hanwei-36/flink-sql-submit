package com.guren.sqlsubmit.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author: HanWei
 * @create: 2021-11-15
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str, String regex) throws Exception {
        if(regex.equals("err")){
            throw new Exception();
        }
        for (String s : str.split(regex)) {
            collect(Row.of(s, s.length()));
        }
    }
}
