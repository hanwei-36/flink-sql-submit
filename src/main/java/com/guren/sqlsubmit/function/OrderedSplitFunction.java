package com.guren.sqlsubmit.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.*;

/**
 * @author: HanWei
 * @create: 2021-11-15
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class OrderedSplitFunction extends ScopedFunction<Row> {
    private static boolean order;

    private static int compare(Row row1, Row row2) {
        int result = (int) row1.getField(1) - (int) row2.getField(1);
        return order ? -result : result  ;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        order = context.getJobParameter(getScope() + "." + "order", "desc").equals("desc");
    }

    public void eval(String str, String regex) {
        List<Row> rows = new ArrayList();
        for (String s : str.split(regex)) {
            rows.add(Row.of(s, s.length()));
        }
        Collections.sort(rows, OrderedSplitFunction::compare);
        rows.forEach(row -> collect(row));
    }


}
