package org.apache.spark.sql.jdbc;

public class HiveSQLDialect extends JdbcDialect
{

    private static final long serialVersionUID = -5702852783637310594L;

    private static HiveSQLDialect instance;

    private HiveSQLDialect() {

    }

    public static HiveSQLDialect getInstance() {
        if (instance == null) {
            instance = new HiveSQLDialect();
        }
        return instance;
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:hive2");
    }

    @Override
    public String quoteIdentifier(String colName) {
        return colName;
    }
}
