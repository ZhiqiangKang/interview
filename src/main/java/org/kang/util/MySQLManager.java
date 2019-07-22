package org.kang.util;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * MySQL连接池
 *
 * @author Kang
 * @version 1.0
 * @time 2019-07-21 16:01
 */
public class MySQLManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLManager.class);

    private static final DruidDataSource DRUID_DATA_SOURCE;

    static {
        DRUID_DATA_SOURCE = new DruidDataSource();
        DRUID_DATA_SOURCE.setDriverClassName("com.mysql.cj.jdbc.Driver");
        DRUID_DATA_SOURCE.setUrl("jdbc:mysql://localhost:3306/test");
        DRUID_DATA_SOURCE.setUsername("root");
        DRUID_DATA_SOURCE.setPassword("123456");
    }

    /**
     * 获取连接
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return DRUID_DATA_SOURCE.getConnection();
    }

    /**
     * 释放资源
     * @param closeables
     */
    public static void release(AutoCloseable... closeables){
        for (AutoCloseable closeable : closeables) {
            try {
                if (closeable != null){
                    closeable.close();
                }
            } catch (Exception e) {
                LOGGER.warn("释放连接失败", e);
            }
        }
    }
}