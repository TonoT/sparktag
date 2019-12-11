package com.epoint.ztb.bigdata.tagmg.iface;

import java.util.List;

import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;

public interface DBMetadataService
{

    /**
     * 
     * @return DataSourceConfig
     */
    public Record getDataSourceConfig(String ip, String port, String database, String username, String password);

    /**
     * 获取表信息
     * 
     * @param service
     * @param tablename
     * @return TagmgTableBasicinfo
     */
    public Record getTable(TagCommonDao service, String tablename);

    /**
     * 查询所有表
     * 
     * @param service
     * @param tablename
     *            用于查询
     * @param tablechinesename
     *            用于查询
     * @return TagmgTableBasicinfo
     */
    public List<Record> getTableList(TagCommonDao service, String tablename, String tablechinesename, int first,
            int pageSize, String sortField, String sortOrder, List<String> excludeTableNames);

    /**
     * 获取还没导入的表的数量
     * 
     * @param service
     * @param tablename
     * @param tablechinesename
     * @param excludeTableNames
     * @return
     */
    public int getTableCount(TagCommonDao service, String tablename, String tablechinesename,
            List<String> excludeTableNames);

    /**
     * 查询某张表下所有字段
     * 
     * @param service
     * @param tablename
     * @param columnname
     *            用于查询
     * @param columnchinesename
     *            用于查询
     * @return TagmgTableStruct
     */
    public List<Record> getColumnList(TagCommonDao service, String tablename);

    /**
     * 将数据库数据类型转为指标库数据类型
     * 
     * @param dbtype
     * @return
     */
    public ColumnType DbType2SysType(String dbtype);
    
    /**
     * 查询所有表
     * 
     * @param service
     * @param tablename
     *            用于查询
     * @param tablechinesename
     *            用于查询
     * @return TagmgTableBasicinfo
     */
    public List<Record> getTableList(TagCommonDao service, String colnums, String tablename, int first,
            int pageSize, String sortField, String sortOrder, String where);

    /**
     * 获取还没导入的表的数量
     * 
     * @param service
     * @param tablename
     * @param tablechinesename
     * @param excludeTableNames
     * @return
     */
    public int getTableCount(TagCommonDao service, String tablename, String where);
    
    /**
     * 获取还没导入的表的数量
     * 
     * @param service
     * @param tablename
     * @param tablechinesename
     * @param excludeTableNames
     * @return
     */
    public List<Record> getSqlData(TagCommonDao service, String sql);
    
}
