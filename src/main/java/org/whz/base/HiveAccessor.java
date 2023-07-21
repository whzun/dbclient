package org.whz.base;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveAccessor {

    private static final Logger log = LoggerFactory.getLogger(HiveAccessor.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run()
            {
                HiveAccessor.close();
            }
        });
    }

    public static void initDataSource(Properties p ) {
        String driver = p.getProperty("driverName","org.apache.hive.jdbc.HiveDriver");

        String url=p.getProperty("url");
        if(url==null || url.length()==0) {
            log.error("URL is null!");
            System.exit(1);
        }

        String user = p.getProperty("user", null);
        String password = p.getProperty("password");
        String maxWait = p.getProperty("maxWait","3000");

        if(driver==null||url==null) {
            log.error("jdbc.properties:");
            log.error("\tdb.driver="+driver);
            log.error("\tdb.url="+url);
            log.error("\tdb.user="+user);
            log.error("\tdb.password="+password);
            log.error("\tdb.maxWait="+maxWait);
        }

        BasicDataSource dataSource=null;
        dataSource=new BasicDataSource();
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setUrl(url);
        dataSource.setDriverClassName(driver);
        dataSource.setInitialSize(5);
        dataSource.setMinIdle(1);
        dataSource.setMaxWait(Long.parseLong(maxWait));
        dataSource.setTestOnBorrow(true);
        dataSource.setTestOnReturn(true);
        dataSource.setTestWhileIdle(true);

        ds = new MyDataSource(dataSource);
    }

    public static MyDataSource getDataSource()
    {
        if(ds !=null)
            return ds;
        else
        	log.error("DataSource is not initiallized!");
        	return null;

    }

    public static boolean isReady()
    {
        return true;
    }

    public static void close()
    {
        if(ds!=null)
        {
            try {
                log.info("Bye!");
                ds.close();
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }
    }

    private static MyDataSource ds = null;
}
