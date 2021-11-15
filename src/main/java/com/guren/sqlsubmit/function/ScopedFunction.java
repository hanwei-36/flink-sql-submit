package com.guren.sqlsubmit.function;

import org.apache.flink.table.functions.TableFunction;

/**
 * @author: HanWei
 * @create: 2021-11-15
 */
public abstract class ScopedFunction<T> extends TableFunction<T> {
    private String scope;

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }
}
