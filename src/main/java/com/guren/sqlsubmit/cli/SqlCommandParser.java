package com.guren.sqlsubmit.cli;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;

import java.util.List;

/**
 * @author: HanWei
 * @create: 2021-11-12
 */
public class SqlCommandParser {

    public static List<SqlNode> parse(String sql) throws SqlParseException {
        SqlConformance conformance = FlinkSqlConformance.DEFAULT;
        SqlParser sqlParser = SqlParser.create(sql,SqlParser.config()
                .withParserFactory(FlinkSqlParserFactories.create(conformance))
                .withConformance(conformance)
                .withLex(Lex.JAVA)
                .withIdentifierMaxLength(256));
        return sqlParser.parseStmtList().getList();
    }
}
