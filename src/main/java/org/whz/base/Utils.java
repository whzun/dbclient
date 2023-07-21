package org.whz.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whz.hiveclient.HiveJdbcClient;

public class Utils {

    private static final Logger log = LoggerFactory.getLogger(HiveJdbcClient.class);
    public static <T extends AutoCloseable> void  close(T obj){
        if(obj != null) {
            try {
                obj.close();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }
}
