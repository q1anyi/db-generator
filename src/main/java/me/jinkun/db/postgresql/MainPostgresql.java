package me.jinkun.db.postgresql;

import me.jinkun.db.utils.FtUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: db-generator
 * @description:postgresql逆向word
 * @author: caicy
 * @create: 2021-02-26 20:23
 */
public class MainPostgresql {
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://106.13.102.87:5432/islandandreef", "postgres", "aochensoft@1024");

        List<Map> tableList = getTableList(conn);

        conn.close();

        FtUtil ftUtil = new FtUtil();
        Map map = new HashMap(16);
        map.put("table", tableList);

        ftUtil.generateFile("/", "moban.xml", map, "/Users/q1anyi/Downloads", "user.doc");
    }

    /**
     * 获取数据库中所有表的表名，并添加到列表结构中。
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    public static List getTableList(Connection conn) throws SQLException {
        List<Map> tableList = new ArrayList<Map>();

        String sql = "SELECT relname AS TABNAME,CAST (obj_description (relfilenode,'pg_class') AS VARCHAR) AS COMMENT FROM pg_class C WHERE relkind='r' AND relname NOT LIKE 'pg_%' AND relname NOT LIKE 'sql_%' ORDER BY relname";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Map map = new HashMap(16);
            String TABLE_NAME = rs.getString("TABNAME");
            String COMMENTS = rs.getString("COMMENT");
            map.put("TABLE_NAME", TABLE_NAME);
            map.put("COMMENTS", COMMENTS == null ? "" : COMMENTS.replaceAll("\r|\n\t", "").trim());

            //获取列
            List<Map> columnList = getColumnList(conn, TABLE_NAME);
            map.put("COLUMNS", columnList);

            //if (TABLE_NAME.startsWith("ZJ"))
            tableList.add(map);
            System.out.println("TABLE_NAME ==>" + TABLE_NAME + "  COMMENTS==>" + COMMENTS);
        }
        rs.close();
        ps.close();
        return tableList;
    }

    /**
     * 获取数据表中所有列的列名，并添加到列表结构中。
     *
     * @param conn
     * @param tableName
     * @return
     * @throws SQLException
     */
    public static List getColumnList(Connection conn, String tableName)
            throws SQLException {

        List<Map> columnList = new ArrayList<Map>();

        //SHOW FULL COLUMNS FROM sys_org
        String sql =
                "SELECT DISTINCT A.attnum AS NUM,A.attname AS NAME,A.attnotnull AS NOTNULL,com.description AS COMMENT," +
                        "COALESCE (i.indisprimary,FALSE) AS KEY,concat_ws " +
                        "('',T.typname,SUBSTRING (format_type (A.atttypid,A.atttypmod) FROM '\\(.*\\)')) AS TYPE,A.atthasdef AS DEFAULT FROM " +
                        "pg_attribute A JOIN pg_class pgc ON pgc.oid=A.attrelid LEFT JOIN pg_index i ON (pgc.oid=i.indrelid AND i.indkey [ 0 ] =A.attnum) " +
                        "LEFT JOIN pg_description com ON (pgc.oid=com.objoid AND A.attnum=com.objsubid) LEFT JOIN pg_attrdef def " +
                        "ON (A.attrelid=def.adrelid AND A.attnum=def.adnum) LEFT JOIN pg_type T ON A.atttypid=T.oid WHERE A.attnum> 0 " +
                        "AND pgc.oid=A.attrelid AND pg_table_is_visible (pgc.oid) AND NOT A.attisdropped AND pgc.relname='" + tableName + "' ORDER BY A.attnum";

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Map map = new HashMap(16);

            String COLUMN_NAME = rs.getString("NAME");
            String DATA_TYPE = rs.getString("TYPE");
            String DATA_LENGTH = "";
            if (DATA_TYPE.indexOf("(") != -1) {
                DATA_LENGTH = DATA_TYPE.substring(DATA_TYPE.indexOf("(") + 1, DATA_TYPE.indexOf(")"));
                DATA_TYPE = DATA_TYPE.substring(0, DATA_TYPE.indexOf("("));
            }
            String DATA_DEFAULT = rs.getString("DEFAULT");
            String NULLABLE = rs.getString("NOTNULL");
            String COMMENTS = rs.getString("COMMENT");
            String PRIMARY_KEY = rs.getString("KEY");
            String NUM = rs.getString("NUM");

            map.put("COMMENTS", COLUMN_NAME);
            map.put("INDEX", NUM);
            map.put("DATA_TYPE", DATA_TYPE);
            map.put("DATA_LENGTH", DATA_LENGTH);
            map.put("DATA_DEFAULT", ("t").equals(DATA_DEFAULT) ? "有" : "无");
            map.put("NULLABLE", ("t").equals(NULLABLE) ? "是" : "否");
            map.put("COLUMN_NAME", COMMENTS == null ? "" : COMMENTS);
            map.put("PRIMARY_KEY", ("t").equals(PRIMARY_KEY) ? "主键" : "");
            columnList.add(map);

            System.out.println("COLUMN_NAME ==>" + COLUMN_NAME + "  DATA_TYPE==>" + DATA_TYPE + "  DATA_LENGTH==>" + DATA_LENGTH + " NULLABLE==>" + NULLABLE + "  COMMENTS==>" + COMMENTS
//                    + " PRIMARY_KEY==>" + PRIMARY_KEY
            );
        }
        rs.close();
        ps.close();
        return columnList;
    }

}
