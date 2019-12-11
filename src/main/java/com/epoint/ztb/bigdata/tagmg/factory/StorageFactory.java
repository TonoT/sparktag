package com.epoint.ztb.bigdata.tagmg.factory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.epoint.ztb.bigdata.tagmg.annotation.StorageType;
import com.epoint.ztb.bigdata.tagmg.common.PkgScanner;
import com.epoint.ztb.bigdata.tagmg.constants.SaveType;
import com.epoint.ztb.bigdata.tagmg.constants.SysConstant;
import com.epoint.ztb.bigdata.tagmg.iface.ResultStorage;

public class StorageFactory
{
    private static Map<String, ResultStorage> services = new HashMap<String, ResultStorage>();

    public static void init() {
        if (services.isEmpty()) {
            PkgScanner scanner = new PkgScanner(SysConstant.PKG_NAME);
            List<String> dbservices = scanner.scan(StorageType.class);
            for (String dbservice : dbservices) {
                try {
                    Class<?> clazz = Class.forName(dbservice);
                    StorageType type = clazz.getAnnotation(StorageType.class);
                    if (StringUtils.isNotBlank(type.value())) {
                        Object obj = clazz.newInstance();
                        if (obj instanceof ResultStorage) {
                            services.put(type.value(), (ResultStorage) obj);
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
    public static ResultStorage getResultStorage(SaveType type) {
        init();
        return services.get(type.getValue());
    }
}
