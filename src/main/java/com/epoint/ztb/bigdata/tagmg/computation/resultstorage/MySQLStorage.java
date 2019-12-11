package com.epoint.ztb.bigdata.tagmg.computation.resultstorage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.spark_project.jetty.util.StringUtil;

import com.epoint.ztb.bigdata.tagmg.annotation.StorageType;
import com.epoint.ztb.bigdata.tagmg.common.DBPwdEncoder;
import com.epoint.ztb.bigdata.tagmg.common.DSConfig;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.computation.common.ComputationalUtils;
import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;
import com.epoint.ztb.bigdata.tagmg.constants.PostprocessingType;
import com.epoint.ztb.bigdata.tagmg.constants.SysConstant;
import com.epoint.ztb.bigdata.tagmg.iface.ResultStorage;

import scala.collection.JavaConverters;
import scala.collection.Seq;

@StorageType("mysql")
public class MySQLStorage implements ResultStorage
{

    private static final long serialVersionUID = 4226136518459256337L;

    private transient SparkSession spark;
    private transient JavaSparkContext jsc;

    private String tablename;
    private Record runconfig;
    private Properties properties = new Properties();

    @Override
    public void init(SparkSession spark, String tablename, Record runconfig) {
        this.spark = spark;
        this.tablename = tablename;
        this.runconfig = runconfig;
        this.jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        properties.put("user", runconfig.getStr("resultusername"));
        properties.put("password", DBPwdEncoder.decode(runconfig.getStr("resultpassword")));
        properties.put("driver", "com.mysql.jdbc.Driver");
    }

    @Override
    public void initTable(boolean recreate, Dataset<Row> keydf, List<Record> tagitems) {
        try(TagCommonDao service = TagCommonDao.getInstance(DSConfig.getDataSourceConfig())){
	        // TODO 判断表是否存在，如果不存在吧recreate改成true
	        if(service.queryInt("select count(1) from information_schema.tables where table_name =?", tablename)<1) {
	        	recreate = true;
	        }
	        if (recreate) {
	            createTable(tagitems, keydf);
	        }
	        else {
	            Dataset<Row> df = spark.read().jdbc(runconfig.getStr("resulturl"), tablename, properties);
	
	            List<String> key = new ArrayList<String>();
	            key.add(keydf.schema().apply(0).name());
	            Seq<String> callID = JavaConverters.asScalaIteratorConverter(key.iterator()).asScala().toSeq();
	
	            List<String> cols = new ArrayList<String>();
	            List<Record> columns = new ArrayList<Record>();
	            for (int i = 0; i < df.schema().length(); i++) {
	                cols.add(df.schema().apply(i).name());
	            }
	            
	            StringBuilder sb1 = new StringBuilder();
	            sb1.append("alter table " + tablename);
	            for (Record record : tagitems) {
	            	// TODO 如果存在 修改列类型和默认值
		            if (cols.contains(record.getStr("itemanothername"))) {
		            	columns.add(record);
		            	String type = record.getStr("returntype");
		            	if("string".equals(type)){
		            		type = "text";
		            	}
	                	sb1.append(" modify column " + record.getStr("itemanothername") + " " + type + ",");
	                }
	            }
	            String sqltype = sb1.toString();
	            sqltype = sqltype.substring(0, sqltype.lastIndexOf(','));
	            service.execute(sqltype);
	            
	            Dataset<Row> joined = keydf.join(df, callID, "leftanti").select(keydf.schema().apply(0).name());
	
	            Broadcast<List<Record>> bc_columns = jsc.broadcast(columns);
	            joined.mapPartitions(new MapPartitionsFunction<Row, Row>()
	            {
	
	                private static final long serialVersionUID = 4589476388153912702L;
	
	                @Override
	                public Iterator<Row> call(Iterator<Row> input) throws Exception {
	                	List<Record> columns = bc_columns.getValue();
	                    try (TagCommonDao dao = TagCommonDao.getInstance()) {
	                        List<Object> datas = new ArrayList<Object>();
	                        String col = "";
	                        while (input.hasNext()) {
	                            Row row = input.next();
	                            datas.add(row.get(0));
	                            col = row.schema().apply(0).name();
	                        }
	                        if (StringUtil.isNotBlank(col)) {
	                            dao.insert(tablename, datas, columns, col);
	                        }
	                    }
	                    return input;
	                }
	
	            }, Encoders.kryo(Row.class)).count();
	
	            bc_columns.unpersist();
	            
	            StringBuilder sb = new StringBuilder();
	            sb.append("alter table " + tablename + " add (");
	            boolean lose = false;
	            for (Record record : tagitems) {
	                if (!cols.contains(record.getStr("itemanothername"))) {
	                    sb.append(record.getStr("itemanothername") + " " + record.getStr("returntype") + " default ");
	                    sb = addDefault(sb, record);
	                    lose = true;
	                }
	            }
	            
	            if (lose) {
	                String sql = sb.toString();
	                sql = sql.substring(0, sql.lastIndexOf(','));
	                sql += ")";
	                service.execute(sql);
	            }
	        }
        }catch (Exception e) {
			e.printStackTrace();
		}

    }

    @Override
    public void reloadColumns(Record tagitem) {
        String columnname = tagitem.getStr("itemanothername");
        String defaultvalue = tagitem.getStr("defaultvalue");
        try (TagCommonDao service = TagCommonDao.getInstance()) {
            service.execute("update " + tablename + " set " + columnname + " = ?", defaultvalue);
        }
    }

    @Override
    public void save(Dataset<Row> df) {
        df.mapPartitions(new MapPartitionsFunction<Row, Row>()
        {

            private static final long serialVersionUID = -1416891576614417693L;

            @Override
            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                List<Record> datas = new ArrayList<Record>();
                boolean b = false;
                Record re = null;
                try (TagCommonDao service = TagCommonDao.getInstance(DSConfig.getDataSourceConfig())) {
                    while (input.hasNext()) {
                        b = true;
                        Row row = input.next();
                        if (row.size() == 3) {
                            re = new Record();
                            re.put("key", row.schema().apply(0).name());
                            re.put(row.schema().apply(0).name(), row.get(0));
                            re.put(row.schema().apply(1).name(), row.get(1));
                            re.put(row.schema().apply(2).name(), row.get(2));
                            datas.add(re);
                        }
                        if (row.size() == 2) {
                            re = new Record();
                            re.put("key", row.schema().apply(0).name());
                            re.put(row.schema().apply(0).name(), row.get(0));
                            re.put(row.schema().apply(1).name(), row.get(1));
                            datas.add(re);
                        }
                    }
                    if (b) {
                        service.update(tablename, datas);
                    }

                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                return input;
            }
        }, Encoders.kryo(Row.class)).count();
    }

    @Override
    public Dataset<Row> get(LinkedHashMap<String, ColumnType> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("(select ");
        for (Entry<String, ColumnType> column : columns.entrySet()) {
            sb.append(column.getKey() + ",");
        }
        String sql = sb.toString();
        sql = sql.substring(0, sql.lastIndexOf(','));
        sql += " from " + tablename + ") as test1";
        return spark.read().jdbc(runconfig.getStr("resulturl"), sql, properties);
    }

    @Override
    public Dataset<Row> getStagingTable(String sql) {
        return ComputationalUtils
                .getTableDF(
                        spark, DSConfig.buildDSConfig(runconfig.getStr("stagingurl"),
                                runconfig.getStr("stagingusername"), runconfig.getStr("stagingpassword")),
                        "(" + sql + ") as dataframetemp");
    }

    @Override
    public void saveStagingTable(String tablename, Dataset<Row> df) {
        try (TagCommonDao service = TagCommonDao.getInstance(DSConfig.getDataSourceConfig())) {
            service.execute("drop table if exists " + tablename);
        }
        recreateMySQLTable(tablename, df.schema()); // 创建内部表

        df.write().mode(SaveMode.Overwrite).jdbc(runconfig.getStr("resulturl"), tablename, properties);
    }

    private void recreateMySQLTable(String tablename2, StructType structtype) {
        StringBuffer sb = new StringBuffer("create table " + tablename2 + "(");

        List<String> mysqlcolumn = new ArrayList<String>(structtype.size());

        for (int i = 0; i < structtype.size(); i++) {
            StructField field = structtype.apply(i);
            String columntype = null;
            DataType datatype = field.dataType();

            if (datatype == DataTypes.IntegerType) {
                columntype = "int";
            }
            else if (datatype == DataTypes.LongType) {
                columntype = "bigint";
            }
            else if (datatype == DataTypes.FloatType) {
                columntype = "float";
            }
            else if (datatype == DataTypes.DoubleType) {
                columntype = "double";
            }
            else if (datatype == DataTypes.StringType) {
                columntype = "text";
            }
            else if (datatype == DataTypes.DateType) {
                columntype = "timestamp";
            }
            else if (datatype == DataTypes.BinaryType) {
                columntype = "binary";
            }
            else {
                columntype = "text";
            }

            mysqlcolumn.add(field.name() + " " + columntype);
        }
        sb.append(StringUtils.join(mysqlcolumn, ","));
        sb.append(")");

        try (TagCommonDao dao = TagCommonDao.getInstance(runconfig.getStr("stagingurl"),
                runconfig.getStr("stagingusername"), DBPwdEncoder.decode(runconfig.getStr("stagingpassword")))) {
            dao.execute("drop table if exists " + tablename2);
            dao.execute(sb.toString());
        }

    }

    private DataType gettypes(String data) {
        if ("double".equals(data)) {
            return DataTypes.DoubleType;
        }
        if ("string".equals(data)) {
            return DataTypes.StringType;
        }
        if ("int".equals(data)) {
            return DataTypes.IntegerType;
        }
        if ("date".equals(data)) {
            return DataTypes.DateType;
        }
        if ("byte".equals(data)) {
            return DataTypes.ByteType;
        }
        return DataTypes.StringType;
    }

    private void createTable(List<Record> tagitems, Dataset<Row> keydf) {
        List<Record> tags = new ArrayList<Record>();
        for (Record record : tagitems) {
            tags.add(record);
            if (!PostprocessingType.不处理.getValue().equals(record.getStr("postprocessingtype"))) {
                record.put("itemanothername", record.get("itemanothername") + SysConstant.RESULT_CTR_VALUE_SUFFIX);
                tags.add(record);
            }
        }
        JavaRDD<Row> rdd = keydf.javaRDD().map(new Function<Row, Row>()
        {

            private static final long serialVersionUID = -2787528867493390766L;

            @Override
            public Row call(Row row) throws Exception {

                Object[] obj = new Object[tags.size() + 1];

                obj[0] = row.get(0);
                for (int i = 1; i < obj.length; i++) {
                    if (StringUtil.isNotBlank(tags.get(i - 1).getStr("defaultvalue"))) {
                        if ("int".equals(tags.get(i - 1).getStr("returntype"))) {
                            obj[i] = Integer.parseInt(tags.get(i - 1).getStr("defaultvalue"));
                        }
                        else if ("double".equals(tags.get(i - 1).getStr("returntype"))) {
                            obj[i] = Double.parseDouble(tags.get(i - 1).getStr("defaultvalue"));
                        }
                        else {
                            obj[i] = tags.get(i - 1).getStr("defaultvalue");
                        }
                    }
                    else {
                        if ("int".equals(tags.get(i - 1).getStr("returntype"))) {
                            obj[i] = 0;
                        }
                        else if ("double".equals(tags.get(i - 1).getStr("returntype"))) {
                            obj[i] = 0.00;
                        }
                        else {
                            obj[i] = "";
                        }
                    }
                }

                return RowFactory.create(obj);
            }

        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(
                DataTypes.createStructField(keydf.schema().apply(0).name(), keydf.schema().apply(0).dataType(), true));
        for (Record record : tags) {
            structFields.add(DataTypes.createStructField(record.getStr("itemanothername"),
                    gettypes(record.getStr("returntype")), true));
        }
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> ds = spark.createDataFrame(rdd, structType);

        ds.write().mode(SaveMode.Overwrite).jdbc(runconfig.getStr("resulturl"), tablename, properties);
    }
    
    private StringBuilder addDefault(StringBuilder sb, Record record) {
    	if (StringUtil.isNotBlank(record.getStr("defaultvalue"))) {
            if ("int".equals(record.getStr("returntype"))) {
                sb.append(Integer.parseInt(record.getStr("defaultvalue")) + ",");
            }
            else if ("double".equals(record.getStr("returntype"))) {
                sb.append(Double.parseDouble(record.getStr("defaultvalue")) + ",");
            }
            else {
                sb.append(record.getStr("defaultvalue") + ",");
            }
        }
        else {
            if ("int".equals(record.getStr("returntype"))) {
                sb.append(0 + ",");
            }
            else if ("double".equals(record.getStr("returntype"))) {
                sb.append(0.00 + ",");
            }
            else {
                sb.append(",");
            }
        }
    	return sb;
    }

}
