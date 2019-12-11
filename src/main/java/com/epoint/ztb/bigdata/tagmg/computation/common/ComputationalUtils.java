package com.epoint.ztb.bigdata.tagmg.computation.common;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.epoint.ztb.bigdata.tagmg.common.DSConfig;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.constants.AccessType;
import com.epoint.ztb.bigdata.tagmg.constants.CodeType;
import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;

public class ComputationalUtils
{

    public static StructType getTagItemStructType(Record keyitem, Record tagitem) {
        StructField[] fields = new StructField[2];
        fields[0] = DataTypes.createStructField(keyitem.getStr("itemanothername"),
                ComputationalUtils.getDataType(ColumnType.getEnumByValue(keyitem.getStr("returntype"))), true);
        fields[1] = DataTypes.createStructField(tagitem.getStr("itemanothername"),
                ComputationalUtils.getDataType(ColumnType.getEnumByValue(tagitem.getStr("returntype"))), true);
        return DataTypes.createStructType(fields);
    }

    public static DataType getDataType(ColumnType returntype) {
        switch (returntype) {
            case 整型:
                return DataTypes.LongType;
            case 浮点型:
                return DataTypes.DoubleType;
            case 字符型:
                return DataTypes.StringType;
            case 时间型:
                return DataTypes.DateType;
            case 字节型:
                return DataTypes.BinaryType;
        }
        return DataTypes.StringType;
    }

    /**
     * 根据引用关系重新排序
     * 
     * @param tagitems
     *            Map<TagmgTagItem, List<TagmgTagItemParam>>
     * @return
     */
    public static Map<Record, List<Record>> resortTagItems(Map<Record, List<Record>> tagitems) {
        Map<String, Record> itemmap = new LinkedHashMap<String, Record>();
        for (Entry<Record, List<Record>> entry : tagitems.entrySet()) {
            itemmap.put(entry.getKey().getStr("itemanothername"), entry.getKey());
        }

        List<String> link = new LinkedList<String>();
        for (Entry<Record, List<Record>> entry : tagitems.entrySet()) {
            link.add(entry.getKey().getStr("itemanothername"));
            if (entry.getValue() != null) {
                for (Record param : entry.getValue()) {
                    if ("tagitem".equals(param.get("accesstype"))) {
                        Record item = itemmap.get(param.get("paramvalue"));
                        if (item != null) {
                            link.add(item.getStr("itemanothername"));
                            resortTagItems(tagitems.get(item), itemmap, tagitems, link);
                        }
                    }
                }
            }
        }

        Map<Record, List<Record>> result = new LinkedHashMap<Record, List<Record>>();
        for (int i = link.size() - 1; i >= 0; i--) {
            Record item = itemmap.get(link.get(i));
            if (!result.containsKey(item)) {
                result.put(item, tagitems.get(item));
            }
        }
        return result;
    }

    /**
     * 
     * @param tagitem
     *            TagmgTagItem
     * @param params
     *            List<TagmgTagItemParam>
     * @param itemmap
     *            Map<String, TagmgTagItem>
     * @param tagitems
     *            Map<TagmgTagItem, List<TagmgTagItemParam>>
     * @param link
     *            List<String>
     */
    private static void resortTagItems(List<Record> params, Map<String, Record> itemmap,
            Map<Record, List<Record>> tagitems, List<String> link) {
        if (params != null) {
            for (Record param : params) {
                if ("tagitem".equals(param.get("accesstype"))) {
                    Record item = itemmap.get(param.get("paramvalue"));
                    if (item != null) {
                        link.add(item.getStr("itemanothername"));
                        resortTagItems(tagitems.get(item), itemmap, tagitems, link);
                    }
                }
            }
        }
    }

    public static Record isolateKeyItem(Map<Record, List<Record>> tagitems) {
        // 获取主键指标项 并从指标项列表中删除主键项
        Record keyitem = null;
        Iterator<Record> it = tagitems.keySet().iterator();
        while (it.hasNext()) {
            Record tagitem = it.next();
            if ("key".equals(tagitem.get("itemtype"))) {
                keyitem = tagitem;
                it.remove();
                break;
            }
        }
        return keyitem;
    }

    public static Dataset<Row> getTableDF(SparkSession spark, Record dsconfig, String tablename) {
        Properties properties = new Properties();
        properties.put("user", dsconfig.get("username"));
        properties.put("password", dsconfig.get("password"));
        properties.put("driver", dsconfig.get("driver"));
        return spark.read().jdbc(dsconfig.getStr("url"), tablename, properties);
    }

    public static JSONObject getPostprocessingJSON(String postprocessing) {
        JSONObject postprocessingjson = null;
        if (StringUtils.isNotBlank(postprocessing)) {
            postprocessingjson = JSON.parseObject(postprocessing);
        }
        else {
            postprocessingjson = new JSONObject();
        }

        // 将空值删除
        Iterator<Entry<String, Object>> it = postprocessingjson.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Object> item = it.next();
            if (item.getValue() == null || "".equals(item.getValue())) {
                it.remove();
            }
        }
        return postprocessingjson;
    }

    /**
     * 
     * 获取参数值
     * 
     * @param row
     * @param param
     * @return
     */
    public static Object getParamValue(Row row, Record param) {
        switch (AccessType.getEnumByValue(param.getStr("accesstype"))) {
            case 变量:
            case 常量:
                return getParamValue_Constant(param);
            case 指标项:
                int index = row.fieldIndex(param.getStr("paramvalue"));
                Object value = null;
                switch (ColumnType.getEnumByValue(param.getStr("paramtype"))) {
                    case 整型:
                        try {
                            value = row.getLong(index);
                        }
                        catch (Exception e1) {
                            try {
                                value = Long.parseLong(row.getString(index));
                            }
                            catch (Exception e2) {
                                value = 0;
                            }
                        }
                        break;
                    case 浮点型:
                        try {
                            value = row.getDouble(index);
                        }
                        catch (Exception e1) {
                            try {
                                value = Double.parseDouble(row.getString(index));
                            }
                            catch (Exception e2) {
                                value = 0.0;
                            }
                        }
                        break;
                    default:
                        break;
                }
                return value;
        }

        return null;
    }

    /**
     * 获取常数、变量参数值
     * 
     * @param param
     * @return
     */
    public static Object getParamValue_Constant(Record param) {
        Object value = null;
        switch (ColumnType.getEnumByValue(param.getStr("paramtype"))) {
            case 整型:
                try {
                    value = param.getInt("paramvalue");
                }
                catch (Exception e) {
                    value = 0;
                }
            case 浮点型:
                try {
                    value = param.getDouble("paramvalue");
                }
                catch (Exception e) {
                    value = 0.0;
                }
                break;
            case 时间型:
                try {
                    value = param.getDate("paramvalue");
                }
                catch (Exception e) {
                    value = new Date(0);
                }
                break;
            default:
                value = param.getStr("paramvalue");
                break;
        }
        return value;
    }

    /**
     * 获取代码项Map
     * 
     * @param codename
     * @param dsconfig
     * @return
     */
    public static Map<String, String> getCodeMap(String codename, Record dsconfig) {
        Map<String, String> codemap = new HashMap<String, String>();
        if (StringUtils.isNotBlank(codename)) {
            Record dsconfigForCode = null;
            if (codename.startsWith(CodeType.数据源.getValue() + CodeType.SPLIT)) {
                dsconfigForCode = dsconfig;
                codename = codename.substring(CodeType.数据源.getValue().length() + CodeType.SPLIT.length());
            }
            else {
                dsconfigForCode = DSConfig.getDataSourceConfig();
                if (codename.startsWith(CodeType.指标库.getValue() + CodeType.SPLIT)) {
                    codename = codename.substring(CodeType.指标库.getValue().length() + CodeType.SPLIT.length());
                }
            }
            try (TagCommonDao service = TagCommonDao.getInstance(dsconfigForCode)) {
                List<Record> codes = service.findList(
                        "select itemvalue, itemtext from code_items where codeid in (select codeid from code_main where codename = ?)",
                        codename);
                for (Record code : codes) {
                    codemap.put(code.getStr("itemvalue"), code.getStr("itemtext"));
                }
            }
        }
        return codemap;
    }
}
