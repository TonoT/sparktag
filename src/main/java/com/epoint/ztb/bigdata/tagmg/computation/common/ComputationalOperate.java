package com.epoint.ztb.bigdata.tagmg.computation.common;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.spark_project.jetty.util.StringUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.constants.AccessType;
import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;
import com.epoint.ztb.bigdata.tagmg.constants.ExtractType;
import com.epoint.ztb.bigdata.tagmg.constants.ItemType;
import com.epoint.ztb.bigdata.tagmg.constants.KeyType;
import com.epoint.ztb.bigdata.tagmg.constants.PostprocessingType;
import com.epoint.ztb.bigdata.tagmg.constants.RunStatus;
import com.epoint.ztb.bigdata.tagmg.constants.SaveType;
import com.epoint.ztb.bigdata.tagmg.constants.SysConstant;
import com.epoint.ztb.bigdata.tagmg.factory.DBMetadataFactory;
import com.epoint.ztb.bigdata.tagmg.factory.FormulaFactory;
import com.epoint.ztb.bigdata.tagmg.factory.StorageFactory;
import com.epoint.ztb.bigdata.tagmg.iface.ResultStorage;
import com.epoint.ztb.bigdata.tagmg.iface.TagItemContent_ReturnMap;
import com.epoint.ztb.bigdata.tagmg.iface.TagItemContent_ReturnRDD;
import com.epoint.ztb.bigdata.tagmg.service.TagItemService;

import scala.Tuple2;

public class ComputationalOperate implements Serializable
{
    private static final long serialVersionUID = 2799701184065298749L;

    private transient ComputationalLogger logger = ComputationalLogger.getLogger(getClass());

    private transient SparkSession spark;
    private transient SparkContext sc;
    private transient JavaSparkContext jsc;

    public ComputationalOperate(SparkSession spark) {
        this.spark = spark;
        this.sc = spark.sparkContext();
        this.jsc = JavaSparkContext.fromSparkContext(sc);
    }

    public List<String> operate(String modelguid) {

        List<String> msgs = new LinkedList<String>();
        Date lastoperatedate = null;
        List<ComputationalBean> beans = null;
        Record runconfig = null;

        boolean isDebug = false;

        try (TagCommonDao service = TagCommonDao.getInstance()) {

            runconfig = getRunConfig(service, modelguid);

            if (runconfig != null) {
                isDebug = runconfig.getInt("isdebug") == 1;
                logger.setDebug(isDebug);// 修改调试模式

                beans = getComputationalBeanList(service, modelguid, msgs);

                lastoperatedate = getLastOperateDate(service, modelguid);
            }
            else {
                String msg = "未找到运行配置对象！模型标识:" + modelguid;
                ComputationalLogger.print(msg);
                msgs.add(msg);
            }
        }
        catch (Exception e) {
            String msg = "获取模型信息时发生异常！模型标识:" + modelguid + "，异常信息：" + e.getMessage();
            ComputationalLogger.print(msg, e);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            msgs.add(msg + "，异常信息：" + sw.getBuffer().toString());
        }

        if (beans != null) {
            for (ComputationalBean bean : beans) {
                Record tag = bean.getTag();
                Record dsconfig = bean.getDataSourceConfig();
                Map<Record, List<Record>> stagingtables = bean.getStagingTables();
                Map<Record, List<Record>> tagitems = bean.getTagItems();

                // 结果存储表名
                String tablename = tag.getStr("taganothername");
                try {
                    // 获取主键指标项 并从指标项列表中删除主键项

                    Record keyitem = ComputationalUtils.isolateKeyItem(tagitems);
                    if (keyitem == null) {
                        String msg = "未找到主键指标项！taganothername：" + tablename;
                        logger.error(msg);
                        msgs.add(msg);
                        continue;
                    }

                    ResultStorage rstorage = StorageFactory
                            .getResultStorage(SaveType.getEnumByValue(runconfig.getStr("savetype")));

                    rstorage.init(spark, tablename, runconfig);

                    Dataset<Row> df_key = buildtagitem(dsconfig, rstorage, keyitem, keyitem, null, lastoperatedate,
                            stagingtables);

                    // 初始化表 创建表 默认值赋值
                    rstorage.initTable(ExtractType.清除后全量.getValue() == keyitem.getInt("extracttype"), df_key,
                            new ArrayList<Record>(tagitems.keySet()));

                    // 根据引用关系重新排序
                    tagitems = ComputationalUtils.resortTagItems(tagitems);

                    // 遍历运算
                    for (Entry<Record, List<Record>> entry : tagitems.entrySet()) {
                        Record tagitem = entry.getKey();
                        try {
                            Dataset<Row> df = buildtagitem(dsconfig, rstorage, keyitem, tagitem, entry.getValue(),
                                    lastoperatedate, stagingtables)
                                            .where(keyitem.getStr("itemanothername") + " is not null or "
                                                    + keyitem.getStr("itemanothername") + " <>  ''")
                                            .distinct();

                            if (ExtractType.清除后全量.getValue() == tagitem.getInt("extracttype")) {
                                // 指标项清除后全量，即删除列
                                rstorage.reloadColumns(tagitem);
                            }

                            if (isDebug) {
                                df.printSchema();
                                df.show();
                            }
                            // 计算
                            rstorage.save(df);
                        }
                        catch (Exception e) {
                            String msg = "计算指标项发生异常！指标项：" + tagitem.getStr("itemanothername");
                            logger.error(msg, e);

                            StringWriter sw = new StringWriter();
                            PrintWriter pw = new PrintWriter(sw);
                            e.printStackTrace(pw);
                            msgs.add(msg + "，异常信息：" + sw.getBuffer().toString());
                        }
                    }
                }
                catch (Exception e) {
                    String msg = "指标计算时发生异常，指标标识：" + tablename;
                    logger.error(msg, e);

                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    msgs.add(msg + "，异常信息：" + sw.getBuffer().toString());
                }
            }
        }

        return msgs;
    }

    private Record getRunConfig(TagCommonDao service, String modelguid) {
        return service.find(
                "select * from tagmg_runconfig where configguid in (select configguid from tagmg_model where modelguid = ?)",
                modelguid);
    }

    private Date getLastOperateDate(TagCommonDao service, String modelguid) {
        Record dateRec = service.find(
                "select operatedate from tagmg_runlog where modelguid = ? and runstatus = ? order by operatedate desc",
                modelguid, RunStatus.执行成功.getValue());
        Date lastoperatedate = null;
        if (dateRec != null) {
            lastoperatedate = dateRec.getDate("operatedate");
        }
        if (lastoperatedate == null) {
            lastoperatedate = new Date(0);
        }
        return lastoperatedate;
    }

    private List<ComputationalBean> getComputationalBeanList(TagCommonDao service, String modelguid,
            List<String> msgs) {

        List<ComputationalBean> beans = new LinkedList<ComputationalBean>();

        List<Record> tagguidRecs = service
                .findList("select tagguid from tagmg_model_tag where modelguid = ? order by orderno desc", modelguid);

        for (Record tagguidRec : tagguidRecs) {
            String tagguid = tagguidRec.getStr("tagguid");
            // 获取指标对象
            Record tag = service.find("select * from tagmg_tag where tagguid = ?", tagguid);
            if (tag == null) {
                String msg = "未找到指标对象！指标标识：" + tagguid;
                logger.error(msg);
                msgs.add(msg);
                continue;
            }

            // 获取数据源信息对象
            Record datasource = service.find("select * from tagmg_datasource where datasourceguid = ?",
                    tag.get("datasourceguid"));
            if (datasource == null) {
                String msg = "未找到指标对应的数据源信息！指标标识：" + tagguid;
                logger.error(msg);
                msgs.add(msg);
                continue;
            }
            Record dsconfig = DBMetadataFactory.getDataSourceConfig(datasource);

            // 获取中间表配置
            List<Record> tables = service.findList("select * from tagmg_stagingtable where tagguid = ?", tagguid);
            Map<Record, List<Record>> stagingtables = new HashMap<Record, List<Record>>();
            for (Record table : tables) {
                List<Record> params = service.findList(
                        "select * from tagmg_stagingtable_param where stagingtableguid = ? order by orderno desc",
                        table.getStr("stagingtableguid"));
                stagingtables.put(table, params);
            }

            // 获取所有指标项
            Map<Record, List<Record>> tagitems = new TagItemService(service).getTagItems(tagguid, true);
            if (tagitems.isEmpty()) {
                String msg = "未找到指标项！指标标识：" + tagguid;
                logger.error(msg);
                msgs.add(msg);
                continue;
            }

            beans.add(new ComputationalBean(tag, dsconfig, stagingtables, tagitems));
        }
        return beans;
    }

    private void buildstagingtable(Record dsconfig, ResultStorage rstorage, Record stagingtable, List<Record> params) {
        logger.info("创建中间表 —— " + stagingtable.get("stagingtablename") + "("
                + stagingtable.getStr("stagingtableanothername") + ")");

        String sql = stagingtable.getStr("sqlcontent");
        for (Record param : params) {
            sql = sql.replace("#" + param.getStr("paramname"), param.getStr("paramvalue"));
        }
        Dataset<Row> df_stagingtable = ComputationalUtils.getTableDF(spark, dsconfig, "(" + sql + ") as dataframetemp");

        rstorage.saveStagingTable(stagingtable.getStr("stagingtableanothername"), df_stagingtable);
    }

    private Dataset<Row> buildtagitem(Record dsconfig, ResultStorage rstorage, Record keyitem, Record tagitem,
            List<Record> params, Date lastoperatedate, Map<Record, List<Record>> stagingtables) {
        logger.info("开始计算指标项 —— " + tagitem.get("itemname"));

        // 创建中间表
        boolean fromstagingtable = false;
        if (StringUtils.isNotBlank(tagitem.getStr("stagingtableguids"))) {
            String[] stagingtableguids = tagitem.getStr("stagingtableguids").split(",");

            for (String stagingtableguid : stagingtableguids) {
                Iterator<Record> it = stagingtables.keySet().iterator();
                while (it.hasNext()) {
                    Record stagingtable = it.next();
                    if (stagingtableguid.equals(stagingtable.getStr("stagingtableguid"))) {
                        buildstagingtable(dsconfig, rstorage, stagingtable, stagingtables.get(stagingtable));
                        // 计算完后删除
                        it.remove();
                    }
                }
            }
            fromstagingtable = true;
        }

        // 指标项计算逻辑
        Dataset<Row> df_tagitem = null;
        switch (ItemType.getEnumByValue(tagitem.getStr("itemtype"))) {
            case 主键:
                df_tagitem = getTagItemDF_Key(dsconfig, keyitem, lastoperatedate);
                break;
            case 基础:
                df_tagitem = getTagItemDF_Basic(dsconfig, tagitem, lastoperatedate, keyitem.getStr("itemanothername"));
                break;
            case SQL:
                df_tagitem = getTagItemDF_SQL(fromstagingtable, dsconfig, rstorage, tagitem, lastoperatedate,
                        keyitem.getStr("itemanothername"), tagitem.getStr("itemanothername"));
                break;
            case 方法:
                df_tagitem = getTagItemDF_Method(dsconfig, keyitem, tagitem, params, lastoperatedate);
                break;
            case 公式:
                df_tagitem = getTagItemDF_Formula(rstorage, keyitem, tagitem, params);
                break;
        }

        // 过滤空值等值
        Column column = df_tagitem.col(tagitem.getStr("itemanothername")).isNotNull();
        String[] excludes = new String[] {"", "NaN", "Infinity", "null", "Null", "NULL" };
        for (String exclude : excludes) {
            column = column.or(column.notEqual(exclude));
        }
        df_tagitem = df_tagitem.where(column);

        // 二次处理
        df_tagitem = postprocessing(df_tagitem, tagitem.getStr("itemattribute"), tagitem.getStr("postprocessingtype"),
                tagitem.getStr("postprocessing"));

        return df_tagitem;
    }

    private Dataset<Row> getTagItemDF_Key(Record dsconfig, Record keyitem, Date lastoperatedate) {
        Dataset<Row> df_new = null;
        switch (KeyType.getEnumByValue(keyitem.getStr("keytype"))) {
            case SQL:
                df_new = ComputationalUtils.getTableDF(spark, dsconfig,
                        "(" + keyitem.getStr("itemcontent") + ") as dataframetemp");
                break;
            case 基础:
                df_new = ComputationalUtils.getTableDF(spark, dsconfig, keyitem.getStr("tablename"));
                if (StringUtils.isNotBlank(keyitem.getStr("filtercondition"))) {
                    df_new = df_new.where(keyitem.getStr("filtercondition"));
                }
                break;
        }
        if (ExtractType.增量.getValue() == keyitem.getInt("extracttype")
                && StringUtils.isNotBlank(keyitem.getStr("incrementstructname"))) {
            df_new = df_new.where(df_new.col(keyitem.getStr("incrementstructname")).gt(lastoperatedate));
        }
        df_new = df_new.select(keyitem.getStr("itemcontent")).withColumnRenamed(keyitem.getStr("itemcontent"),
                keyitem.getStr("itemanothername")).where(keyitem.getStr("itemanothername") + " is not null or "
                        + keyitem.getStr("itemanothername") + " <>  ''")
        	      .distinct();

        return df_new;
    }

    private Dataset<Row> getTagItemDF_Basic(Record dsconfig, Record tagitem, Date lastoperatedate, String keyname) {
        Dataset<Row> df_tagitem = ComputationalUtils.getTableDF(spark, dsconfig, tagitem.getStr("tablename"));
        if (StringUtils.isNotBlank(tagitem.getStr("filtercondition"))) {
            df_tagitem = df_tagitem.where(tagitem.getStr("filtercondition"));
        }
        if (ExtractType.增量.getValue() == tagitem.getInt("extracttype")
                && StringUtils.isNotBlank(tagitem.getStr("incrementstructname"))) {
            df_tagitem = df_tagitem.where(df_tagitem.col(tagitem.getStr("incrementstructname")).gt(lastoperatedate));
        }
        df_tagitem = df_tagitem.select(tagitem.getStr("associatedstructname"), tagitem.getStr("itemcontent"))
                .withColumnRenamed(tagitem.getStr("associatedstructname"), keyname)
                .withColumnRenamed(tagitem.getStr("itemcontent"), tagitem.getStr("itemanothername"));
        return df_tagitem;
    }

    private Dataset<Row> getTagItemDF_SQL(boolean fromstagingtable, Record dsconfig, ResultStorage rstorage,
            Record tagitem, Date lastoperatedate, String keyname, String columnname) {

        Dataset<Row> df_tagitem = null;
        List<Record> datas = null;
        String sql = "";
        try(TagCommonDao service = TagCommonDao.getInstance()) {
        	datas = service.findList("select paramname,paramvalue from tagmg_tag_item_param where itemguid=?", tagitem.getStr("itemguid"));
			sql = tagitem.getStr("itemcontent");
	        for (Record data : datas) {
				sql = sql.replace("#"+data.getStr("paramname"), data.getStr("paramvalue"));
			}
        }
        catch (Exception e) {
        	String msg = "获取参数信息时发生异常！参数标识:" + tagitem.getStr("itemguid") + "，异常信息：" + e.getMessage();
            ComputationalLogger.print(msg, e);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
		}
        
        if (fromstagingtable) {
            df_tagitem = rstorage.getStagingTable(sql);
        }
        else {
            df_tagitem = ComputationalUtils.getTableDF(spark, dsconfig,
                    "(" + sql + ") as dataframetemp");
        }

        if (df_tagitem.schema().size() >= 2) {
            if (ExtractType.增量.getValue() == tagitem.getInt("extracttype")
                    && StringUtils.isNotBlank(tagitem.getStr("incrementstructname"))) {
                // 增量过滤
                if (df_tagitem.schema().size() >= 3) {
                    if (df_tagitem.schema().apply(2).dataType() == DataTypes.DateType) {
                        df_tagitem = df_tagitem
                                .where(df_tagitem.col(df_tagitem.schema().apply(2).name()).gt(lastoperatedate));
                    }
                    df_tagitem.drop(df_tagitem.columns()[2]);
                }
            }

            df_tagitem = df_tagitem.select(df_tagitem.col(df_tagitem.columns()[0]).as(keyname),
                    df_tagitem.col(df_tagitem.columns()[1]).as(columnname));
        }

        return df_tagitem;
    }

    private Dataset<Row> getTagItemDF_Method(Record dsconfig, Record keyitem, Record tagitem, List<Record> params,
            Date lastoperatedate) {
        Dataset<Row> df_tagitem = null;
        try {
            Class<?> clazz = Class.forName(tagitem.getStr("itemcontent"));
            Object contentobj = clazz.newInstance();

            Map<String, Object> paramMap = new HashMap<String, Object>();
            for (Record param : params) {
                paramMap.put(param.getStr("paramname"), ComputationalUtils.getParamValue_Constant(param));
            }

            Dataset<Row> df_source = ComputationalUtils.getTableDF(spark, dsconfig, tagitem.getStr("tablename"));

            if (ExtractType.增量.getValue() == tagitem.getInt("extracttype")
                    && StringUtils.isNotBlank(tagitem.getStr("incrementstructname"))) {
                df_source = df_source.where(df_source.col(tagitem.getStr("incrementstructname")).gt(lastoperatedate));
            }

            if (contentobj instanceof TagItemContent_ReturnMap) {
                // 列表接口
                TagItemContent_ReturnMap itemcontent = (TagItemContent_ReturnMap) contentobj;
                JavaRDD<Record> rdd = df_source.toJavaRDD().map(new Function<Row, Record>()
                {

                    private static final long serialVersionUID = -4688946715065952855L;

                    @Override
                    public Record call(Row row) throws Exception {
                        Record rec = new Record();
                        scala.collection.Iterator<StructField> it = row.schema().iterator();
                        while (it.hasNext()) {
                            StructField field = it.next();
                            String name = field.name();
                            rec.put(name, row.get(row.fieldIndex(name)));
                        }
                        return rec;
                    }
                });
                Map<Object, Object> callresult = null;
                try (TagCommonDao dao = TagCommonDao.getInstance(dsconfig)) {
                    callresult = itemcontent.call(tagitem, dao, rdd.collect(), paramMap);
                }

                if (callresult != null) {
                    List<Row> rows = new ArrayList<Row>(callresult.size());
                    for (Entry<Object, Object> entry : callresult.entrySet()) {
                        rows.add(RowFactory.create(entry.getKey(), entry.getValue()));
                    }
                    df_tagitem = spark.createDataFrame(jsc.parallelize(rows),
                            ComputationalUtils.getTagItemStructType(keyitem, tagitem));
                }

            }
            else if (contentobj instanceof TagItemContent_ReturnRDD) {
                // 分布式接口
                TagItemContent_ReturnRDD itemcontent = (TagItemContent_ReturnRDD) contentobj;

                JavaRDD<Row> rdd = itemcontent.call(tagitem, spark, df_source, paramMap)
                        .map(new Function<Tuple2<Object, Object>, Row>()
                        {
                            private static final long serialVersionUID = 2603480299241024464L;

                            public Row call(Tuple2<Object, Object> t) throws Exception {

                                return RowFactory.create(t._1, t._2);
                            }
                        });

                df_tagitem = spark.createDataFrame(rdd, ComputationalUtils.getTagItemStructType(keyitem, tagitem))
                        .distinct();
            }
            else {
                logger.info("[" + tagitem.get("itemcontent") + "] 不是实现类");
            }
        }
        catch (Exception e) {
            logger.error("调用方法报错！方法：" + tagitem.getStr("itemcontent"), e);
        }
        return df_tagitem;
    }

    private Dataset<Row> getTagItemDF_Formula(ResultStorage rstorage, Record keyitem, Record tagitem,
            List<Record> params) {

        LinkedHashMap<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(keyitem.getStr("itemanothername"), ColumnType.getEnumByValue(keyitem.getStr("renturntype")));
        for (Record param : params) {
            if (AccessType.指标项.getValue().equals(param.getStr("accesstype"))) {
                columns.put(param.getStr("paramvalue"), ColumnType.getEnumByValue(param.getStr("renturntype")));
            }
        }

        // 获取主键和引用的指标项
        Dataset<Row> df = rstorage.get(columns);

        Broadcast<Record> bc_tagitem = jsc.broadcast(tagitem);
        Broadcast<List<Record>> bc_params = jsc.broadcast(params);

        JavaRDD<Row> rdd = df.javaRDD().map(new Function<Row, Row>()
        {

            private static final long serialVersionUID = -8073253437252800351L;

            @Override
            public Row call(Row row) throws Exception {
                Logger logger = Logger.getLogger(getClass());

                Record tagitem = bc_tagitem.getValue();
                List<Record> params = bc_params.getValue();

                Object val = null;
                Map<String, Object> paramMap = new HashMap<String, Object>();
                try {
                    for (Record param : params) {
                        paramMap.put(param.getStr("paramname"), ComputationalUtils.getParamValue(row, param));
                    }
                    val = FormulaFactory.runFormula(tagitem.getStr("itemcontent"), tagitem.getStr("returntype"),
                            paramMap);
                }
                catch (Exception e) {
                    logger.error("公式计算报错！公式：" + tagitem.get("itemcontent") + "，参数：" + JSON.toJSONString(paramMap), e);
                }

                return RowFactory.create(row.get(0), val);
            }
        });
        bc_tagitem.unpersist();
        bc_params.unpersist();

        return spark.createDataFrame(rdd, ComputationalUtils.getTagItemStructType(keyitem, tagitem));
    }

    /**
     * 二次处理及 代码项渲染
     * 
     * @return
     */
    private Dataset<Row> postprocessing(Dataset<Row> df, String pn, String postprocessingtype, String postprocessing) {

        if (StringUtil.isNotBlank(postprocessing) && StringUtil.isNotBlank(postprocessingtype)
                && !PostprocessingType.不处理.getValue().equals(postprocessingtype)) {

            JSONObject postprocessingjson = ComputationalUtils.getPostprocessingJSON(postprocessing);
            Broadcast<String> bc_pn = jsc.broadcast(pn);
            Broadcast<JSONObject> bc_postprocessingjson = jsc.broadcast(postprocessingjson);

            JavaRDD<Row> rdd = df.toJavaRDD().map(new Function<Row, Row>()
            {

                private static final long serialVersionUID = 5335604928863981317L;

                @Override
                public Row call(Row row) throws Exception {
                    int index = row.length() - 1;

                    Object valobj = row.get(index);

                    double v = 0.0;
                    if (valobj != null) {
                        if (valobj instanceof String) {
                            try {
                                v = Double.parseDouble(valobj.toString());
                            }
                            catch (Exception e) {

                            }
                        }
                        else if (valobj instanceof Double || valobj instanceof Long || valobj instanceof Integer
                                || valobj instanceof Float) {
                            v = (double) valobj;
                        }
                        else if (valobj instanceof Date) {
                            v = ((Date) valobj).getTime();
                        }
                    }

                    // 二次处理
                    JSONObject postprocessingjson = bc_postprocessingjson.getValue();

                    switch (PostprocessingType.getEnumByValue(postprocessingtype)) {
                        case 权重型:
                            if (StringUtils.isNotBlank(postprocessingjson.getString("standard"))) {
                                // 标准值
                                double standard = postprocessingjson.getDouble("standard");
                                if ("-".equals(bc_pn.getValue())) {
                                    // 负向
                                    v = v == 0 ? 0 : standard / v;
                                }
                                else {
                                    // 正向
                                    v = standard == 0 ? 0 : v / standard;
                                }
                            }
                            if (StringUtils.isNotBlank(postprocessingjson.getString("weight"))) {
                                // 权重
                                double weight = postprocessingjson.getDouble("weight");
                                v = v * weight;
                            }
                            v = v * 100;
                            break;
                        case 数值型:
                            if (StringUtils.isNotBlank(postprocessingjson.getString("min"))) {
                                // 最小值
                                double min = postprocessingjson.getDouble("min");
                                v = v > min ? v : min;
                            }
                            if (StringUtils.isNotBlank(postprocessingjson.getString("max"))) {
                                // 最大值
                                double max = postprocessingjson.getDouble("max");
                                v = v < max ? v : max;
                            }
                            break;
                        default:
                            break;
                    }

                    // 浮点型
                    if (row.schema().apply(index).dataType() == DataTypes.LongType) {
                        valobj = Math.round(v);
                    }
                    else {
                        valobj = v;
                    }

                    Object[] objs = new Object[row.length() + 1];
                    for (int i = 0; i < row.length(); i++) {
                        objs[i] = row.get(i);
                    }
                    objs[row.length()] = valobj;

                    return RowFactory.create(objs);
                }
            });
            bc_pn.unpersist();
            bc_postprocessingjson.unpersist();

            int length = df.schema().size();

            StructField sf = df.schema().apply(length - 1);
            String name = sf.name();

            StructField[] fields = new StructField[length + 1];
            for (int i = 0; i < length - 1; i++) {
                fields[i] = df.schema().apply(i);
            }
            fields[length - 1] = DataTypes.createStructField(name + SysConstant.RESULT_CTR_VALUE_SUFFIX, sf.dataType(),
                    sf.nullable());
            fields[length] = sf;

            df = spark.createDataFrame(rdd, DataTypes.createStructType(fields));
        }
        return df;
    }
}
