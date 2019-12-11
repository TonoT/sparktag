package com.epoint.ztb.bigdata.tagmg.common;

import java.util.ResourceBundle;

public class DSConfig
{
    private static Record dsconfig;

    public static Record getDataSourceConfig() {
        ResourceBundle bundle = ResourceBundle.getBundle("jdbc");
        if (dsconfig == null) {
            dsconfig = buildDSConfig(bundle.getString("url"), bundle.getString("username"),
                    bundle.getString("password"));
        }
        return dsconfig;
    }

    public static Record buildDSConfig(String url, String username, String password) {
        Record dbconfig = new Record();
        dbconfig.put("url", url);
        dbconfig.put("username", username);
        String pwd = password;
        try {
            pwd = DBPwdEncoder.decode(password);
        }
        catch (Exception e) {

        }
        dbconfig.put("password", pwd);
        dbconfig.put("driver", getDriver(url));
        return dbconfig;
    }

    public static String getDriver(String url) {
        String driver = null;
        if (url.startsWith("jdbc:mysql://")) {
            driver = "com.mysql.jdbc.Driver";
        }
        else if (url.startsWith("jdbc:oracle:thin:")) {
            driver = "oracle.jdbc.driver.OracleDriver";
        }
        else if (url.startsWith("jdbc:sqlserver://")) {
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }
        else if (url.startsWith("jdbc:hive2://")) {
            driver = "org.apache.hive.jdbc.HiveDriver";
        }
        return driver;
    }
}
