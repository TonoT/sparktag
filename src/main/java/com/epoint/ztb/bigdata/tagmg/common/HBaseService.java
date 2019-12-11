package com.epoint.ztb.bigdata.tagmg.common;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.epoint.ztb.bigdata.tagmg.constants.SysConstant;

public class HBaseService implements AutoCloseable
{
    private Connection connection;

    private HBaseService(Configuration config) {
        try {
            this.connection = ConnectionFactory.createConnection(config);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HBaseService getInstance(Configuration config) {

        return new HBaseService(config);
    }

    public Connection getConnection() {
        return connection;
    }

    public ResultScanner scan(String tablename) {
        return scan(tablename, null, -1);
    }

    public ResultScanner scan(String tablename, int first, int pageSize) {
        String startRow = null;
        if (first > 0) {
            ResultScanner beforeScanner = scan(tablename, null, first + 1);

            Result res = null;
            for (Result result : beforeScanner) {
                res = result;
            }
            startRow = Bytes.toString(res.getRow());
        }
        return scan(tablename, startRow, pageSize);
    }

    public ResultScanner scan(String tablename, String startRow, int pageSize) {
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            // 获取表对象
            table = connection.getTable(TableName.valueOf(tablename));
            Scan scan = new Scan();

            if (StringUtils.isNotBlank(startRow)) {
                scan.setStartRow(Bytes.toBytes(startRow));
            }
            if (pageSize > 0) {
                PageFilter filter = new PageFilter(pageSize);
                scan.setFilter(filter);
            }

            resultScanner = table.getScanner(scan);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (table != null) {
                try {
                    table.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return resultScanner;
    }

    public List<String> getColumns(String tablename) {
        List<String> columns = new LinkedList<String>();

        Table table = null;
        try {
            // 获取表对象
            table = connection.getTable(TableName.valueOf(tablename));
            Scan scan = new Scan();

            PageFilter filter = new PageFilter(1);
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                // 循环遍历结果集的数据 当然是没有任何数据
                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(SysConstant.HBASE_FAMILY));
                for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                    columns.add(Bytes.toString(entry.getKey()));
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (table != null) {
                try {
                    table.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return columns;
    }

    public List<Result> get(String tablename, List<String> keyValues, List<String> columns) {
        Table table = null;
        List<Result> results = new LinkedList<Result>();
        try {
            // 获取表对象
            table = connection.getTable(TableName.valueOf(tablename));
            for (String keyValue : keyValues) {
                Get get = new Get(Bytes.toBytes(keyValue));
                if (columns != null && !columns.isEmpty()) {
                    for (String column : columns) {
                        get.addColumn(Bytes.toBytes(SysConstant.HBASE_FAMILY), Bytes.toBytes(column));
                    }
                }
                else {
                    get.addFamily(Bytes.toBytes(SysConstant.HBASE_FAMILY));
                }
                results.add(table.get(get));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (table != null) {
                try {
                    table.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return results;
    }

    public void insert(String tablename, List<Put> puts) {
        Table table = null;
        try {
            // 获取表对象
            table = connection.getTable(TableName.valueOf(tablename));
            table.put(puts);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (table != null) {
                try {
                    table.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void truncate(String tablename) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);
            if (admin.tableExists(tableName)) {
                if (admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }
                admin.truncateTable(tableName, false);
                admin.enableTable(tableName);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (admin != null) {
                try {
                    admin.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void deleteTable(String tablename, boolean recreate) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);
            if (admin.tableExists(tableName)) {
                if (admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }
                admin.deleteTable(tableName);
            }

            if (recreate) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(SysConstant.HBASE_FAMILY);
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (admin != null) {
                try {
                    admin.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 创建表
     * 
     * @param tablename
     * @param recreate
     *            true：表存在时删除后重建；false：表存在时启用表
     */
    public void createTable(String tablename, boolean recreate) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);

            boolean iscreate = true;
            if (admin.tableExists(tableName)) {
                if (recreate) {
                    if (admin.isTableEnabled(tableName)) {
                        admin.disableTable(tableName);
                    }
                    admin.deleteTable(tableName);
                }
                else {
                    if (admin.isTableDisabled(tableName)) {
                        admin.enableTable(tableName);
                        iscreate = false;
                    }
                }
            }

            if (iscreate) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(SysConstant.HBASE_FAMILY);
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (admin != null) {
                try {
                    admin.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void deleteColumns(String tablename, String column) {
        Table table = null;
        try {
            TableName tableName = TableName.valueOf(tablename);
            table = connection.getTable(tableName);
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            List<Delete> deletes = new LinkedList<Delete>();
            for (Result result : scanner) {
                Delete delete = new Delete(result.getRow());
                delete.addColumns(Bytes.toBytes(SysConstant.HBASE_FAMILY), Bytes.toBytes(column));
                deletes.add(delete);
            }
            table.delete(deletes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (table != null) {
                try {
                    table.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static Configuration getHBaseConfig(String resulturl) {
        Configuration config = HBaseConfiguration.create();

        if (StringUtils.isNotBlank(resulturl)) {
            String[] quorumConfig = resulturl.split("@");
            String quorum = quorumConfig[0], znode = null;
            if (quorumConfig.length > 1) {
                znode = quorumConfig[1];
            }

            config.set("hbase.zookeeper.quorum", quorum);
            if (StringUtils.isNotBlank(znode)) {
                config.set("zookeeper.znode.parent", znode);
            }
        }
        return config;
    }
}
