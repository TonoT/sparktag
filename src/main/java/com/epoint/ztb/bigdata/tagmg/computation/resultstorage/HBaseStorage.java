package com.epoint.ztb.bigdata.tagmg.computation.resultstorage;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.HiveSQLDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.epoint.ztb.bigdata.tagmg.annotation.StorageType;
import com.epoint.ztb.bigdata.tagmg.common.DSConfig;
import com.epoint.ztb.bigdata.tagmg.common.HBaseService;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.computation.common.ComputationalUtils;
import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;
import com.epoint.ztb.bigdata.tagmg.constants.PostprocessingType;
import com.epoint.ztb.bigdata.tagmg.constants.RunType;
import com.epoint.ztb.bigdata.tagmg.constants.SysConstant;
import com.epoint.ztb.bigdata.tagmg.iface.ResultStorage;

import scala.Tuple2;

@StorageType("hbase")
public class HBaseStorage implements ResultStorage
{

    private static final long serialVersionUID = 2466552191016204135L;

    private transient SparkSession spark;
    private transient JavaSparkContext jsc;

    private String tablename;
    private Record runconfig;

    @Override
    public void init(SparkSession spark, String tablename, Record runconfig) {

        this.spark = spark;
        this.jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        this.tablename = tablename;
        this.runconfig = runconfig;
    }

    @Override
    public void initTable(boolean recreate, Dataset<Row> keydf, List<Record> tagitems) {

        try (HBaseService service = HBaseService
                .getInstance(HBaseService.getHBaseConfig(runconfig.getStr("resulturl")))) {
            // 创建表
            service.createTable(tablename, recreate);
        }

        Broadcast<List<Record>> bc_tagitems = jsc.broadcast(tagitems);
        Broadcast<String> bc_resulturl = jsc.broadcast(runconfig.getStr("resulturl"));
        Broadcast<String> bc_tablename = jsc.broadcast(tablename);
        keydf.mapPartitions(new MapPartitionsFunction<Row, Row>()
        {

            private static final long serialVersionUID = 4131719718086463947L;

            @Override
            public Iterator<Row> call(Iterator<Row> it) throws Exception {
                try (HBaseService service = HBaseService
                        .getInstance(HBaseService.getHBaseConfig(bc_resulturl.getValue()))) {
                    Connection connection = service.getConnection();

                    byte[] familtbyt = Bytes.toBytes(SysConstant.HBASE_FAMILY);

                    List<Record> tagitems = bc_tagitems.getValue();
                    String tablename = bc_tablename.getValue();

                    Table table = connection.getTable(TableName.valueOf(tablename));

                    List<Put> puts = new LinkedList<Put>();
                    while (it.hasNext()) {
                        Row row = it.next();
                        byte[] keyByt = getBytes(row, 0);

                        Get get = new Get(keyByt);
                        get.addFamily(familtbyt);
                        Result result = table.get(get);

                        Put put = new Put(keyByt);

                        for (Record tagitem : tagitems) {
                            String defaultvalue = tagitem.getStr("defaultvalue");

                            byte[] columnnamebyt = Bytes.toBytes(tagitem.getStr("itemanothername"));
                            if (!PostprocessingType.不处理.getValue().equals(tagitem.getStr("postprocessingtype"))) {
                                byte[] columnnamectrbyt = Bytes.toBytes(
                                        tagitem.getStr("itemanothername") + SysConstant.RESULT_CTR_VALUE_SUFFIX);
                                if (!result.containsColumn(familtbyt, columnnamectrbyt) || StringUtils
                                        .isBlank(Bytes.toString(result.getValue(familtbyt, columnnamectrbyt)))) {
                                    put.addColumn(familtbyt, columnnamectrbyt,
                                            defaultvalue == null ? null : Bytes.toBytes(defaultvalue));
                                }
                            }
                            if (!result.containsColumn(familtbyt, columnnamebyt)
                                    || StringUtils.isBlank(Bytes.toString(result.getValue(familtbyt, columnnamebyt)))) {
                                put.addColumn(familtbyt, columnnamebyt,
                                        defaultvalue == null ? null : Bytes.toBytes(defaultvalue));
                            }
                        }
                        if (!put.isEmpty()) {
                            puts.add(put);
                        }
                    }
                    if (!puts.isEmpty()) {
                        table.put(puts);
                    }
                }
                return it;
            }

        }, Encoders.kryo(Row.class)).count();

        bc_tagitems.unpersist();
        bc_resulturl.unpersist();
        bc_tablename.unpersist();
    }

    @Override
    public void reloadColumns(Record tagitem) {
        String columnname = tagitem.getStr("itemanothername");
        String defaultvalue = tagitem.getStr("defaultvalue");
        Configuration config = HBaseService.getHBaseConfig(runconfig.getStr("resulturl"));
        config.set(TableInputFormat.INPUT_TABLE, tablename);
        JavaPairRDD<ImmutableBytesWritable, Result> pairrdd = jsc.newAPIHadoopRDD(config, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);

        JavaRDD<byte[]> rdd = pairrdd.map(new Function<Tuple2<ImmutableBytesWritable, Result>, byte[]>()
        {

            private static final long serialVersionUID = 3661485547059649994L;

            @Override
            public byte[] call(Tuple2<ImmutableBytesWritable, Result> result) throws Exception {

                return result._2.getRow();
            }

        });

        Broadcast<String> bc_tablename = jsc.broadcast(tablename);
        Broadcast<String> bc_columnname = jsc.broadcast(columnname);
        Broadcast<String> bc_defaultvalue = jsc.broadcast(defaultvalue);
        rdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, byte[]>()
        {

            private static final long serialVersionUID = -7009011922065007922L;

            @Override
            public Iterator<byte[]> call(Iterator<byte[]> it) throws Exception {
                try (HBaseService service = HBaseService
                        .getInstance(HBaseService.getHBaseConfig(runconfig.getStr("resulturl")))) {
                    List<Put> puts = new LinkedList<Put>();
                    while (it.hasNext()) {
                        byte[] row = it.next();
                        Put put = new Put(row);
                        put.addColumn(Bytes.toBytes(SysConstant.HBASE_FAMILY), Bytes.toBytes(bc_columnname.getValue()),
                                Bytes.toBytes(bc_defaultvalue.getValue()));
                        puts.add(put);
                    }
                    service.insert(bc_tablename.getValue(), puts);
                }
                return it;
            }
        }).count();
        bc_tablename.unpersist();
        bc_columnname.unpersist();
        bc_defaultvalue.unpersist();
    }

    @Override
    public Dataset<Row> get(LinkedHashMap<String, ColumnType> columns) {
        Configuration config = HBaseService.getHBaseConfig(runconfig.getStr("resulturl"));
        config.set(TableInputFormat.INPUT_TABLE, tablename);
        JavaPairRDD<ImmutableBytesWritable, Result> pairrdd = jsc.newAPIHadoopRDD(config, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);

        Broadcast<Map<String, ColumnType>> bc_columns = jsc.broadcast(columns);

        JavaRDD<Row> rdd = pairrdd.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>()
        {

            private static final long serialVersionUID = -1200603548676240957L;

            @Override
            public Row call(Tuple2<ImmutableBytesWritable, Result> result) throws Exception {
                Map<String, ColumnType> columns = bc_columns.getValue();

                Map<byte[], byte[]> familyMap = result._2.getFamilyMap(Bytes.toBytes(SysConstant.HBASE_FAMILY));

                Object[] vals = new Object[familyMap.size() + 1];

                // 主键
                vals[0] = Bytes.toString(result._2.getRow());

                int i = 0;
                for (Entry<String, ColumnType> entry : columns.entrySet()) {
                    if (i > 0) {
                        byte[] keybyt = Bytes.toBytes(entry.getKey());

                        String value = Bytes.toString(familyMap.get(keybyt));

                        switch (entry.getValue()) {
                            case 整型:
                                vals[i] = StringUtils.isBlank(value) ? 0 : Long.parseLong(value);
                                break;
                            case 浮点型:
                                vals[i] = StringUtils.isBlank(value) ? 0.0 : Double.parseDouble(value);
                                break;
                            case 字符型:
                                vals[i] = StringUtils.isBlank(value) ? "" : value;
                                break;
                            case 时间型:
                                vals[i] = StringUtils.isBlank(value) ? null : new Date(Long.parseLong(value));
                                break;
                            case 字节型:
                                vals[i] = value;
                                break;
                        }
                    }
                    i++;
                }
                return RowFactory.create(vals);
            }

        });

        bc_columns.unpersist();

        StructField[] fields = new StructField[columns.size()];
        int i = 0;
        for (Entry<String, ColumnType> entry : columns.entrySet()) {
            fields[i++] = DataTypes.createStructField(entry.getKey(), ComputationalUtils.getDataType(entry.getValue()),
                    true);
        }

        return spark.createDataFrame(rdd, DataTypes.createStructType(fields));
    }

    @Override
    public void save(Dataset<Row> df) {
        Broadcast<String> bc_resulturl = jsc.broadcast(runconfig.getStr("resulturl"));
        Broadcast<String> bc_tablename = jsc.broadcast(tablename);

        df.mapPartitions(new MapPartitionsFunction<Row, Row>()
        {

            private static final long serialVersionUID = 5691312826942367394L;

            @Override
            public Iterator<Row> call(Iterator<Row> it) throws Exception {
                try (HBaseService service = HBaseService
                        .getInstance(HBaseService.getHBaseConfig(bc_resulturl.getValue()))) {
                    String tablename = bc_tablename.getValue();
                    Connection connection = service.getConnection();

                    Table table = connection.getTable(TableName.valueOf(tablename));

                    List<Put> puts = new LinkedList<Put>();
                    while (it.hasNext()) {
                        Row row = it.next();

                        byte[] keybyt = getBytes(row, 0);
                        Get get = new Get(keybyt);
                        get.addFamily(Bytes.toBytes(SysConstant.HBASE_FAMILY));

                        if (!table.get(get).isEmpty()) {
                            // 该行键存在
                            Put put = new Put(keybyt);
                            for (int i = 1; i < row.size(); i++) {
                                put.addColumn(Bytes.toBytes(SysConstant.HBASE_FAMILY),
                                        Bytes.toBytes(row.schema().apply(i).name()), getBytes(row, i));
                            }
                            puts.add(put);
                        }
                    }
                    table.put(puts);
                }
                return it;
            }

        }, Encoders.kryo(Row.class)).count();

        bc_resulturl.unpersist();
        bc_tablename.unpersist();
    }

    @Override
    public Dataset<Row> getStagingTable(String sql) {
        Dataset<Row> df = null;
        if (RunType.本地模式.getValue().equals(runconfig.getStr("runtype"))) {
            JdbcDialects.registerDialect(HiveSQLDialect.getInstance());
            df = ComputationalUtils
                    .getTableDF(
                            spark, DSConfig.buildDSConfig(runconfig.getStr("stagingurl"),
                                    runconfig.getStr("stagingusername"), runconfig.getStr("stagingpassword")),
                            "(" + sql + ") as dataframetemp");
            JdbcDialects.unregisterDialect(HiveSQLDialect.getInstance());
        }
        else {
            df = spark.sql(sql);
        }
        return df;
    }

    @Override
    public void saveStagingTable(String tablename, Dataset<Row> df) {

        try (HBaseService service = HBaseService
                .getInstance(HBaseService.getHBaseConfig(runconfig.getStr("resulturl")))) {
            service.deleteTable(tablename, true);
        }
        recreateHiveExternalTable(tablename, df.schema()); // 创建内部表

        Broadcast<String> bc_resulturl = jsc.broadcast(runconfig.getStr("resulturl"));
        Broadcast<String> bc_tablename = jsc.broadcast(tablename);
        df.mapPartitions(new MapPartitionsFunction<Row, Row>()
        {

            private static final long serialVersionUID = 5691312826942367394L;

            @Override
            public Iterator<Row> call(Iterator<Row> it) throws Exception {
                try (HBaseService service = HBaseService
                        .getInstance(HBaseService.getHBaseConfig(bc_resulturl.getValue()))) {
                    String tablename = bc_tablename.getValue();
                    Connection connection = service.getConnection();

                    Table table = connection.getTable(TableName.valueOf(tablename));

                    List<Put> puts = new LinkedList<Put>();
                    while (it.hasNext()) {
                        Row row = it.next();

                        // 该行键存在
                        Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                        for (int i = 0; i < row.size(); i++) {
                            put.addColumn(Bytes.toBytes(SysConstant.HBASE_FAMILY),
                                    Bytes.toBytes(row.schema().apply(i).name()), getBytes(row, i));
                        }
                        puts.add(put);
                    }
                    table.put(puts);
                }
                return it;
            }

        }, Encoders.kryo(Row.class)).count();

        bc_resulturl.unpersist();
        bc_tablename.unpersist();

    }

    private void recreateHiveExternalTable(String tablename, StructType structtype) {

        StringBuffer sb = new StringBuffer("create external table " + tablename + "(");

        List<String> hivecolumn = new ArrayList<String>(structtype.size());
        List<String> hbasecolumn = new ArrayList<String>(structtype.size());
        hivecolumn.add(SysConstant.HIVE_HBASE_KEYNAME + " string");
        hbasecolumn.add(":key");

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
                columntype = "string";
            }
            else if (datatype == DataTypes.DateType) {
                columntype = "timestamp";
            }
            else if (datatype == DataTypes.BinaryType) {
                columntype = "binary";
            }
            else {
                columntype = "string";
            }

            hivecolumn.add(field.name() + " " + columntype);
            hbasecolumn.add(SysConstant.HBASE_FAMILY + ":" + field.name());
        }
        sb.append(StringUtils.join(hivecolumn, ","));
        sb.append(")");
        sb.append(" STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'");
        sb.append(" WITH SERDEPROPERTIES('hbase.columns.mapping'='" + StringUtils.join(hbasecolumn, ",") + "')");
        sb.append(" TBLPROPERTIES('hbase.table.name'='" + tablename + "')");

        try (TagCommonDao dao = TagCommonDao.getInstance(runconfig.getStr("stagingurl"),
                runconfig.getStr("stagingusername"), runconfig.getStr("stagingpassword"))) {
            dao.execute("drop table if exists " + tablename);
            dao.execute(sb.toString());
        }
    }

    private byte[] getBytes(Row row, int fieldIndex) {
        if (row.get(fieldIndex) != null) {
            DataType datatype = row.schema().apply(fieldIndex).dataType();
            if (datatype == DataTypes.LongType) {
                return Bytes.toBytes(String.valueOf(row.getLong(fieldIndex)));
            }
            else if (datatype == DataTypes.DoubleType) {
                return Bytes.toBytes(String.valueOf(row.getDouble(fieldIndex)));
            }
            else if (datatype == DataTypes.StringType) {
                return Bytes.toBytes(row.getString(fieldIndex));
            }
            else if (datatype == DataTypes.DateType) {
                return Bytes.toBytes(String.valueOf(row.getDate(fieldIndex).getTime()));
            }
            else if (datatype == DataTypes.BinaryType) {
                return row.getAs(fieldIndex);
            }
            else {
                return Bytes.toBytes(row.get(fieldIndex).toString());
            }
        }
        return null;
    }
}
