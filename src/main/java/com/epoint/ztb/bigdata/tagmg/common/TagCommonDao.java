package com.epoint.ztb.bigdata.tagmg.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import com.alibaba.fastjson.JSON;

import jodd.util.StringUtil;

public class TagCommonDao implements AutoCloseable {
	private Logger logger = Logger.getLogger(getClass());

	private Record dbConfig;

	private Connection conn;

	private TagCommonDao(String url, String username, String password) {
		try {
			dbConfig = DSConfig.buildDSConfig(url, username, password);
			Class.forName(dbConfig.getStr("driver"));
			conn = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static TagCommonDao getInstance() {

		return getInstance(DSConfig.getDataSourceConfig());
	}

	public static TagCommonDao getInstance(Record dsconfig) {

		return new TagCommonDao(dsconfig.getStr("url"), dsconfig.getStr("username"), dsconfig.getStr("password"));
	}

	public static TagCommonDao getInstance(String url, String username, String password) {

		return new TagCommonDao(url, username, password);
	}

	public List<Record> findList(String sql, Object... params) {
		List<Record> result = new ArrayList<Record>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sql);
			for (int i = 1; i <= params.length; i++) {
				ps.setObject(i, params[i - 1]);
			}
			rs = ps.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int col = rs.getMetaData().getColumnCount();
			while (rs.next()) {
				Record map = new Record();
				for (int i = 1; i <= col; i++) {
					map.put(metaData.getColumnName(i), rs.getObject(i));
				}
				result.add(map);
			}
		} catch (SQLException e) {
			logger.debug("SQL查询发生异常！" + sql + ";参数：" + JSON.toJSONString(params), e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.debug("数据库关闭异常！", e);
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.debug("数据库查询结果关闭异常！", e);
				}
			}
		}
		return result;
	}

	public Record find(String sql, Object... params) {
		Record result = new Record();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sql);
			for (int i = 1; i <= params.length; i++) {
				ps.setObject(i, params[i - 1]);
			}
			rs = ps.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int col = rs.getMetaData().getColumnCount();
			if (rs.next()) {
				for (int i = 1; i <= col; i++) {
					result.put(metaData.getColumnName(i), rs.getObject(i));
				}
			}
		} catch (SQLException e) {
			logger.debug("SQL查询发生异常！" + sql + ";参数：" + JSON.toJSONString(params), e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.debug("数据库关闭异常！", e);
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.debug("数据库查询结果关闭异常！", e);
				}
			}
		}
		return result;
	}

	/**
     * 获取列名。
     * 
     * @param sql
     *            sql
     * @return 表数据
	 * @throws SQLException 
     */
    public List<String> getColumn(String sql) throws SQLException {
        PreparedStatement pstmt = null;
        List<String> array = new LinkedList<String>();
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            int col = rs.getMetaData().getColumnCount();
            for (int i = 0; i < col; i++) {
            	array.add(rs.getMetaData().getColumnName(i+1));
			}
        }catch (SQLException e) {
        	throw e;
        }
        finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                }
                catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
       
        return array;
    }
	
	
	public String queryString(String sql, Object... params) {
		String i = null;
		Record result = find(sql, params);
		for (Entry<String, Object> entry : result.entrySet()) {
			if (entry.getValue() != null) {
				i = entry.getValue().toString();
			}
			break;
		}
		return i;
	}

	public int queryInt(String sql, Object... params) {
		int i = 0;
		Record result = find(sql, params);
		for (Entry<String, Object> entry : result.entrySet()) {
			try {
				i = Integer.parseInt(entry.getValue().toString());
				break;
			} catch (Exception e) {

			}
		}
		return i;
	}

	public boolean execute(String sql, Object... params) {
		boolean isSuccess = false;
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			for (int i = 1; i <= params.length; i++) {
				ps.setObject(i, params[i - 1]);
			}
			isSuccess = ps.execute();
		} catch (SQLException e) {
			logger.debug("SQL查询发生异常！" + sql + ";参数：" + JSON.toJSONString(params), e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.debug("数据库关闭异常！", e);
				}
			}
		}
		return isSuccess;
	}

	public void insert(String tableName, List<Object> lists, List<Record> columns, String keyname) {
		String colname = keyname+",";
		String colvalue = "?,";
		for(int i=0;i<columns.size();i++) {
			Record column = columns.get(i);
			colname += column.getStr("itemanothername")+",";
			colvalue += "\""+column.getStr("defaultvalue")+"\",";
		}
		colname = colname.substring(0, colname.lastIndexOf(','));
		colvalue = colvalue.substring(0, colvalue.lastIndexOf(','));
		String sql = "insert into " + tableName + "("+colname+") values(" + colvalue + ")";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sql);
			for (Object str : lists) {
				stmt.setObject(1, str);
				stmt.addBatch();
			}
			stmt.executeBatch();
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.debug("数据库关闭异常！", e);
				}
			}
		}
	}
	
	public void update(String tableName, List<Record> recordlist) {
		long l1 = System.currentTimeMillis();
		Record recordT = recordlist.get(0);
        String sql = "update " + tableName + " set ";
        Iterator<String> itorT = recordT.keySet().iterator();
        while (itorT.hasNext()) {
            String name = itorT.next();
            if (!"key".equalsIgnoreCase(name)&&!recordT.getStr("key").equalsIgnoreCase(name)) {
                sql += name + " = ";
                sql += "?,";
            }
        }
        sql = Functions.TrimEnd(sql, ",") + " where " + recordT.getStr("key") + " = ?";
        // }
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(sql);

            for (int i = 0; i < recordlist.size(); i++) {
                Record record = recordlist.get(i);

                int j = 0;
                Iterator<String> itor = record.keySet().iterator();
                while (itor.hasNext()) {
                    String name = itor.next();

                    Object val = record.get(name);
                    if (!"key".equalsIgnoreCase(name)&&!recordT.getStr("key").equalsIgnoreCase(name)) {
                        stmt.setObject(j + 1, val);
                        j++;
                    }
                }
                stmt.setObject(j + 1, record.getStr(record.getStr("key")));
                stmt.addBatch();
            }
            int[] i = stmt.executeBatch();

		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.debug("数据库关闭异常！", e);
				}
			}
		}
        long l2 = System.currentTimeMillis();
        logger.debug("批量修改：" + tableName + "\t" + recordlist.size() + "\t" + (l2 - l1) / 1000.0);

	}
	
	public Record getDbConfig() {
		return dbConfig;
	}

	@Override
	public void close() {
		if (conn != null) {
			try {
				if (!conn.isClosed()) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
