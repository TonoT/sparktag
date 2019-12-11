package com.epoint.ztb.bigdata.tagmg.dbmetadataservice.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.epoint.ztb.bigdata.tagmg.annotation.DBType;
import com.epoint.ztb.bigdata.tagmg.common.DSConfig;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.SqlBuilder;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.constants.ColumnType;
import com.epoint.ztb.bigdata.tagmg.iface.DBMetadataService;

@DBType("hive")
public class HiveMetadataService implements DBMetadataService {

	@Override
	public Record getDataSourceConfig(String ip, String port, String database, String username, String password) {
		if (StringUtils.isBlank(port)) {
			port = "10000";
		}
		String url = new StringBuffer("jdbc:hive2://").append(ip).append(":").append(port).append("/").append(database)
				.toString();
		return DSConfig.buildDSConfig(url, username, password);
	}

	@Override
	public Record getTable(TagCommonDao service, String tablename) {
		SqlBuilder sqlBuilder = buildTableSQL(service, null);
		sqlBuilder.append(" and tablename = ?", tablename);
		return service.find(sqlBuilder.getSql(), sqlBuilder.getParams());
	}

	@Override
	public List<Record> getTableList(TagCommonDao service, String tablename, String tablechinesename, int first,
			int pageSize, String sortField, String sortOrder, List<String> excludeTableNames) {
		List<Record> tables = new ArrayList<>();
		if (StringUtils.isBlank(tablename)) {
			tablename = "";
		}
		if (StringUtils.isBlank(tablechinesename)) {
			tablechinesename = "";
		}
		String sql = "show tables  like '*" + tablechinesename + "*" + tablename + "*'";
		List<Record> tablelist = service.findList(sql);
		int size = tablelist.size();
		for (Record record : tablelist) {
			for (String tableName : excludeTableNames) {
				if (tableName.equalsIgnoreCase(record.getStr("tab_name"))) {
					size--;
				}
			}
		}
		int begin = 0;
		if (first < size) {
			begin = first;
		}
		while (begin < first + pageSize) {
			if (begin < tablelist.size()) {
				String tab_name = tablelist.get(begin).getStr("tab_name");
				boolean flag = true;
				for (String tableName : excludeTableNames) {
					if (tableName.equalsIgnoreCase(tab_name)) {
						flag = false;
						break;
					}
				}
				if (flag) {
					Record re = new Record();
					re.put("tablename", tab_name);
					re.put("tablechinesename", tab_name);
					tables.add(re);
				}
				begin++;
			} else {
				break;
			}
		}
		return tables;
	}

	@Override
	public int getTableCount(TagCommonDao service, String tablename, String tablechinesename,
			List<String> excludeTableNames) {
		if (StringUtils.isBlank(tablename)) {
			tablename = "";
		}
		if (StringUtils.isBlank(tablechinesename)) {
			tablechinesename = "";
		}
		String sql = "show tables  like '*" + tablechinesename + "*" + tablename + "*'";
		List<Record> tablelist = service.findList(sql);
		int size = tablelist.size();
		for (Record record : tablelist) {
			for (String tableName : excludeTableNames) {
				if (tableName.equalsIgnoreCase(record.getStr("tab_name"))) {
					size--;
				}
			}
		}
		return size;
	}

	@Override
	public List<Record> getColumnList(TagCommonDao service, String tablename) {
		String sql = "desc " + tablename;
		List<Record> columns = new ArrayList<>();
		List<Record> colList = service.findList(sql);
		for (Record record : colList) {
			Record re = new Record();
			re.put("columnchinesename", record.getStr("col_name"));
			re.put("columnname", record.getStr("col_name"));
			re.put("columntype", record.getStr("data_type"));
			columns.add(re);
		}
		return columns;
	}

	@Override
	public ColumnType DbType2SysType(String dbtype) {
		if (StringUtils.isNotBlank(dbtype)) {
			switch (dbtype.toLowerCase()) {
			case "binary":
			case "bit":
			case "blob":
			case "longblob":
			case "mediumblob":
			case "tinyblob":
			case "varbinary":
				return ColumnType.字节型;
			case "date":
			case "datetime":
			case "time":
			case "timestamp":
				return ColumnType.时间型;
			case "decimal":
			case "double":
			case "float":
			case "numeric":
			case "year":
				return ColumnType.浮点型;
			case "int":
			case "bigint":
			case "integer":
			case "mediumint":
			case "smallint":
			case "tinyint":
				return ColumnType.整型;
			case "char":
			case "longtext":
			case "mediumtext":
			case "text":
			case "varchar":
			case "tinytext":
			default:
				return ColumnType.字符型;
			}
		}
		return ColumnType.字符型;
	}

	public SqlBuilder buildTableSQL(TagCommonDao service, String columns) {
		SqlBuilder sqlBuilder = new SqlBuilder();

		if (StringUtils.isBlank(columns)) {
			columns = "*";
		}

		sqlBuilder.append("select " + columns + " from (");
		if (service.queryInt(
				"select count(distinct column_name) from information_schema.columns where table_schema = (select database()) and table_name = 'table_basicinfo' and column_name in ('tablename','sql_tablename')") == 2) {
			// 避免F9的table_basicinfo表中存在重复数据，这里采用子查询，不用左连接，优先采用有值的并且是中文的，所以子查询中用倒叙排序，并limit1
			sqlBuilder.append(
					"select distinct table_name as tablename, ifnull((select tablename from table_basicinfo where sql_tablename = information_schema.tables.table_name and tablename <> '' order by tablename desc limit 1), table_name) as tablechinesename from information_schema.tables where table_schema = (select database())");
		} else {
			// 非F9
			sqlBuilder.append(
					"select distinct table_name as tablename, table_name as tablechinesename from information_schema.tables where table_schema = (select database())");
		}
		sqlBuilder.append(") temp where 1=1");
		return sqlBuilder;
	}

	public SqlBuilder buildColumnSQL(TagCommonDao service, String tablename) {
		SqlBuilder sqlBuilder = new SqlBuilder();

		sqlBuilder.append("select * from (");
		if (service.queryInt(
				"select count(distinct column_name) from information_schema.columns where table_schema = (select database()) and table_name = 'table_struct' and column_name in ('fieldname','fieldchinesename')") == 2) {
			// 避免F9的table_struct表中存在重复数据，这里采用子查询，不用左连接，优先采用有值的并且是中文的，所以子查询中用倒叙排序，并limit1
			sqlBuilder.append(
					"select distinct column_name as columnname, ifnull(ifnull((select fieldchinesename from table_struct where fieldname = information_schema.columns.column_name and fieldchinesename <> '' order by fieldchinesename desc limit 1), if(column_comment = '', null, column_comment)), column_name) as columnchinesename, data_type as columntype, ifnull(ifnull(character_maximum_length, numeric_precision), datetime_precision) as columnlength, ifnull(character_octet_length,numeric_scale) as columnprecision from information_schema.columns where table_name = ? and table_schema = (select database())",
					tablename);
		} else {
			// 非F9
			sqlBuilder.append(
					"select distinct column_name as columnname, ifnull(if(column_comment = '', null, column_comment), column_name) as columnchinesename, data_type as columntype, ifnull(ifnull(character_maximum_length, numeric_precision), datetime_precision) as columnlength, ifnull(character_octet_length,numeric_scale) as columnprecision from information_schema.columns where table_name = ? and table_schema = (select database())",
					tablename);
		}
		sqlBuilder.append(") temp where 1=1");
		return sqlBuilder;
	}

	@Override
	public List<Record> getTableList(TagCommonDao service, String colnums, String tablename, int first, int pageSize,
			String sortField, String sortOrder, String where) {
		String sql = "select * from " + tablename + where;
		List<Record> tables = new ArrayList<>();
		List<Record> tablelist = service.findList(sql);
		int begin = 0;
		if (first < tablelist.size()) {
			begin = first;
		}

		while (begin < first + pageSize) {
			if (begin < tablelist.size()) {
				tables.add(tablelist.get(begin));
				begin++;
			} else {
				break;
			}
		}
		return tables;
	}

	@Override
	public int getTableCount(TagCommonDao service, String tablename, String where) {
		String sql = "select * from " + tablename + where;
		List<Record> tablelist = service.findList(sql);
		return tablelist.size();
	}
	
	
	@Override
	public List<Record> getSqlData(TagCommonDao service, String sql) {
		return service.findList(sql);
	}

}
