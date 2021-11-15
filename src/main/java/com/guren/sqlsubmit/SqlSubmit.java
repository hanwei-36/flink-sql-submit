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
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.util.NlsString;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

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
        String sql = Files.lines(Paths.get(workSpace + "/" + sqlFilePath), StandardCharsets.UTF_8).collect(Collectors.joining("\n"));
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
        switch (call.getKind()) {
            case SET_OPTION:
                callSet((SqlSetOption) call);
                break;
            case CREATE_TABLE:
                callCreateTable(call.toString());
                break;
            case INSERT:
                callInsertInto(call.toString());
                break;
            case CREATE_FUNCTION:
                callCreateFunction((SqlCreateFunction) call);
                break;
            default:
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

    private void callSet(SqlSetOption cal) {
        String key = cal.getName().toString();
        String value = cal.getValue().toString();
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
}
