package com.epoint.ztb.bigdata.tagmg.iface;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;

public interface ResultStorage extends Serializable
{

    /**
     * 初始化，将需要的参数先传入对象中
     * 
     * @param spark
     * @param username
     * @param password
     * @param tablename
     */
    public void init(SparkSession spark, String tablename, Record runconfig);

    /**
     * 不存在表的创建表，并创建不存在的列，列为空值时渲染默认值。
     * 
     * @param keydf
     * @param tagitem
     * @param resulturl
     */
    public void initTable(boolean recreate, Dataset<Row> keydf, List<Record> tagitems);

    /**
     * 用于指标项的清除后全量
     * 
     * @param columnname
     */
    public void reloadColumns(Record tagitem);

    /**
     * 获取表数据，用于指标项参数中引用别的指标项获取值时调用
     * 
     * @param tablename
     * @param columns
     */
    public Dataset<Row> get(LinkedHashMap<String, ColumnType> columns);

    /**
     * 保存数据，针对某张表的某几个字段保存
     * 
     * @param df
     */
    public void save(Dataset<Row> df);

    /**
     * 从中间表中获取
     * 
     * @param sql
     * @return
     */
    public Dataset<Row> getStagingTable(String sql);

    /**
     * 保存中间表
     * 
     * @param df
     */
    public void saveStagingTable(String tablename, Dataset<Row> df);
}
