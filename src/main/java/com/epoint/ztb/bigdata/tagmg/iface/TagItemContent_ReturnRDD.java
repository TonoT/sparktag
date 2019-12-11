package com.epoint.ztb.bigdata.tagmg.iface;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.epoint.ztb.bigdata.tagmg.common.Record;

import scala.Tuple2;

public interface TagItemContent_ReturnRDD
{
    /**
     * 
     * @param tagitem
     * @param spark
     * @param data
     *            增量数据
     * @return 返回为 key-value的DF
     */
    public JavaRDD<Tuple2<Object, Object>> call(Record tagitem, SparkSession spark, Dataset<Row> data,
            Map<String, Object> params);
}
