/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.guren.sqlsubmit;

import com.guren.sqlsubmit.cli.CliOptions;
import com.guren.sqlsubmit.cli.CliOptionsParser;
import com.guren.sqlsubmit.cli.SqlCommandParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.NlsString;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SqlSubmit {

    public static void main(String[] args) throws Exception {
        SqlSubmit submit = new SqlSubmit(args);
        submit.run();
    }

    // --------------------------------------------------------------------------------------------

    private String sqlFilePath;
    private String workSpace;
    private TableEnvironment tEnv;
    private StatementSet statementSet;
    private ParameterTool parameterTool;

    private SqlSubmit(String[] args) {
        final CliOptions options = CliOptionsParser.parseClient(args);
        this.parameterTool = ParameterTool.fromArgs(args);
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
    }

    private void run() throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        bsEnv.getConfig().setGlobalJobParameters(parameterTool);
        this.tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        this.statementSet = tEnv.createStatementSet();
        String sql = readSQL(workSpace + "/" + sqlFilePath);
        System.out.println("the whole sql:");
        System.out.println(sql);
        List<SqlNode> calls = SqlCommandParser.parse(sql);
        for (SqlNode call : calls) {
            callCommand(call);
        }
        System.out.println(tEnv.getConfig().getConfiguration());
        this.statementSet.execute();
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlNode call) {
        System.out.println("current sql : " + call.toString());
        if (call instanceof SqlSet) {
            callSet((SqlSet) call);
        } else if (call instanceof SqlCreateTable || call instanceof SqlCreateView) {
            callCreateTable(call.toString());
        } else if (call instanceof SqlCreateFunction) {
            callCreateFunction((SqlCreateFunction) call);
        } else if (call instanceof RichSqlInsert) {
            callInsertInto(call.toString());
        } else {
            throw new RuntimeException("Unsupported command: " + call.toString());
        }
    }

    private void callCreateFunction(SqlCreateFunction call) {
        try {
            String functionName = call.getFunctionIdentifier()[0];
            String className = ((NlsString) call.getFunctionClassName().getValue()).getValue();
            UserDefinedFunction userDefinedFunction = (UserDefinedFunction) Class.forName(className).newInstance();
            tEnv.createTemporaryFunction(functionName, userDefinedFunction);
        } catch (Exception e) {
            throw new RuntimeException("create function failed:\n" + call + "\n", e);
        }
    }

    private void callSet(SqlSet cal) {
        String key = cal.getKeyString();
        String value = cal.getValueString();
        tEnv.getConfig().getConfiguration().setString(key, value);
        // 方便自定义函数获取参数
        tEnv.getConfig().addJobParameter(key, value);
    }

    private void callCreateTable(String cmdCall) {
        try {
            tEnv.executeSql(cmdCall);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + cmdCall + "\n", e);
        }
    }

    private void callInsertInto(String cmdCall) {
        try {
            statementSet.addInsertSql(cmdCall);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + cmdCall + "\n", e);
        }
    }

    private String readSQL(String url) throws Exception {
        try {
            URL file = Path.fromLocalFile(new File(url).getAbsoluteFile())
                    .toUri()
                    .toURL();
            return IOUtils.toString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new Exception(
                    String.format("Fail to read content from the %s.", url), e);
        }
    }
}
