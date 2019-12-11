package com.epoint.ztb.bigdata.tagmg.tagitemcontent.impl;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.epoint.ztb.bigdata.tagmg.annotation.TagItemContent;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.iface.TagItemContent_ReturnRDD;

import scala.Tuple2;

@TagItemContent("TagItemContent_ReturnRDD测试")
public class TagItemContent_ReturnRDD_Test implements TagItemContent_ReturnRDD
{

    @Override
    public JavaRDD<Tuple2<Object, Object>> call(Record tagitem, SparkSession spark, Dataset<Row> data, Map<String, Object> params) {

        return null;
    }

}
