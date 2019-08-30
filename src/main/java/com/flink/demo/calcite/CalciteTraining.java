package com.flink.demo.calcite;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.List;

/**
 * Created by P0007 on 2019/8/29.
 */
public class CalciteTraining {

    public static void main(String[] args) throws SqlParseException {
        String sql = "select id,name,sum(age) as s_age,cast(age as int) as int_age from emps where id = 1";
        SqlParser sqlParser = SqlParser.create(sql);
        SqlNode sqlNode = sqlParser.parseQuery();
        SqlNodeList selectList = ((SqlSelect) sqlNode).getSelectList();
        List<SqlNode> sqlNodes = selectList.getList();
        for (SqlNode node : sqlNodes) {
            String alias = SqlValidatorUtil.getAlias(node, 0);
            System.out.println(alias);
        }


    }

}
