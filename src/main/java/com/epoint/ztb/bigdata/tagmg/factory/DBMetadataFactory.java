package com.epoint.ztb.bigdata.tagmg.factory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.epoint.ztb.bigdata.tagmg.annotation.DBType;
import com.epoint.ztb.bigdata.tagmg.common.PkgScanner;
import com.epoint.ztb.bigdata.tagmg.common.Record;
import com.epoint.ztb.bigdata.tagmg.common.TagCommonDao;
import com.epoint.ztb.bigdata.tagmg.constants.SysConstant;
import com.epoint.ztb.bigdata.tagmg.iface.DBMetadataService;

public class DBMetadataFactory
{
    private static Map<String, DBMetadataService> services = new HashMap<String, DBMetadataService>();

    public static void init() {
        if (services.isEmpty()) {
            PkgScanner scanner = new PkgScanner(SysConstant.PKG_NAME);
            List<String> dbservices = scanner.scan(DBType.class);
            for (String dbservice : dbservices) {
                try {
                    Class<?> clazz = Class.forName(dbservice);
                    DBType type = clazz.getAnnotation(DBType.class);
                    if (StringUtils.isNotBlank(type.value())) {
                        Object obj = clazz.newInstance();
                        if (obj instanceof DBMetadataService) {
                            services.put(type.value(), (DBMetadataService) obj);
                        }
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 
     * @param type
     *            不区分大小写
     */
    public static DBMetadataService getDBMetadataService(String type) {
        init();
        return services.get(type);
    }

    public static Record getDataSourceConfig(Record datasource) {
        DBMetadataService dbmdservice = DBMetadataFactory.getDBMetadataService(datasource.getStr("datasourcetype"));
        Record config = dbmdservice.getDataSourceConfig(datasource.getStr("ip"), datasource.getStr("port"),
                datasource.getStr("databasename"), datasource.getStr("username"), datasource.getStr("password"));
        return config;
    }

    public static TagCommonDao getDataSourceDao(Record datasource) {
        return TagCommonDao.getInstance(getDataSourceConfig(datasource));
    }
}
