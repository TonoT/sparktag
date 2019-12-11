package com.epoint.ztb.bigdata.tagmg.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * SqlBuilder工具类
 * 
 */
public class SqlBuilder
{
    /**
     * 参数化 sql 字符串
     */
    private StringBuffer sqlb = null;
    /**
     * 拼接 sql 字符串
     */
    private StringBuffer sqlbspell = null;
    /**
     * params
     */
    private List<Object> params = null;

    public SqlBuilder() {
        sqlb = new StringBuffer();
        sqlbspell = new StringBuffer();
        params = new ArrayList<Object>();
    }

    public void append(String sql) {
        sqlb.append(sql);
        sqlbspell.append(sql);
    }

    /**
     * 普通sql 参数化写法
     * 
     * @param sql
     *            sql语句
     * @param args
     *            SQL所需参数
     */
    public void append(String sql, Object... args) {
        if (args != null && args.length > 0) {
            sqlb.append(sql);
            for (int i = 0; i < args.length; i++) {
                params.add(args[i]);
            }
        }
        else {
            append(sql);
        }
    }

    /**
     * 处理sql中含有in的参数
     * 
     * @param sql
     * @param 涉及的参数
     */
    public void appendIn(String sql, List<String> groupguidlis) {
        if (groupguidlis == null || groupguidlis.isEmpty()) {
            throw new RuntimeException("参数不能为空");
        }
        else {
            StringBuilder strb = new StringBuilder();
            for (int i = 0; i < groupguidlis.size(); i++) {
                strb.append("?,");
            }
            String s = strb.substring(0, strb.length() - 1);

            sqlb.append(sql.replace("?", s));
            // sqlbspell.append(sql.replace("?", s));
            params.addAll(groupguidlis);
        }
    }

    /**
     * 兼容用逗号分隔的字符串形式，以及含有单引号的形式，
     * 
     * @param sql
     * @param args
     */
    public void appendIn(String sql, String args) {
        if (StringUtils.isBlank(args)) {
            throw new RuntimeException("参数不能为空");
        }

        String[] argArray = args.split(",");

        List<String> argList = new ArrayList<String>();
        for (String arg : argArray) {
            argList.add(Functions.Trim(arg, "'"));
        }

        appendIn(sql, argList);
    }

    /**
     * 得到拼接的Sql字符串(参数化写法)
     * 
     * @return
     */
    public String getSql() {
        return getSql(true);
    }

    /**
     * 得到拼接的Sql字符串
     * useParameterize true 参数化写法 false 拼好的sql写法
     * 
     * @param useParameterize
     * @return
     */
    public String getSql(Boolean useParameterize) {
        if (useParameterize) {
            return sqlb.toString();
        }
        else {
            return sqlbspell.toString();
        }

    }

    /**
     * 获取所传参数值
     * 
     * @return
     */
    public Object[] getParams() {
        return params.toArray();
    }

    /**
     * 一些sql拼接过程中会有重新 内容，需要截取 by 付金鑫 2019年4月22日19:58:42
     * 
     * @param 需要截取开始位置
     * @param 需要截取结束位置
     * @return 截取后的内容
     */
    public StringBuffer deleteSql(int start, int end) {
        sqlb.delete(start, end);
        return sqlb;

    }
}
