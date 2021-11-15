package com.guren.sqlsubmit.cli;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import java.util.List;

/**
 * @author: HanWei
 * @create: 2021-11-12
 */
public class SqlCommandParser {

    public static List<SqlNode> parse(String sql) throws SqlParseException {
        SqlParser sqlParser = SqlParser.create(sql, SqlParser.configBuilder()
                .setParserFactory(FlinkSqlParserImpl.FACTORY)
                .setConformance(FlinkSqlConformance.DEFAULT)
                .setLex(Lex.JAVA)
                .build());
        return sqlParser.parseStmtList().getList();
    }
}
