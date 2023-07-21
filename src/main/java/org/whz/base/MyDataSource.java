package org.whz.base;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

public class MyDataSource extends BasicDataSource implements AutoCloseable {
	private BasicDataSource ds;
	public int hashCode() {
		return ds.hashCode();
	}
	public boolean getDefaultAutoCommit() {
		return ds.getDefaultAutoCommit();
	}
	public void setDefaultAutoCommit(boolean defaultAutoCommit) {
		ds.setDefaultAutoCommit(defaultAutoCommit);
	}
	public boolean getDefaultReadOnly() {
		return ds.getDefaultReadOnly();
	}
	public void setDefaultReadOnly(boolean defaultReadOnly) {
		ds.setDefaultReadOnly(defaultReadOnly);
	}
	public boolean equals(Object obj) {
		return ds.equals(obj);
	}
	public int getDefaultTransactionIsolation() {
		return ds.getDefaultTransactionIsolation();
	}
	public void setDefaultTransactionIsolation(int defaultTransactionIsolation) {
		ds.setDefaultTransactionIsolation(defaultTransactionIsolation);
	}
	public String getDefaultCatalog() {
		return ds.getDefaultCatalog();
	}
	public void setDefaultCatalog(String defaultCatalog) {
		ds.setDefaultCatalog(defaultCatalog);
	}
	public String getDriverClassName() {
		return ds.getDriverClassName();
	}
	public void setDriverClassName(String driverClassName) {
		ds.setDriverClassName(driverClassName);
	}
	public int getMaxActive() {
		return ds.getMaxActive();
	}
	public void setMaxActive(int maxActive) {
		ds.setMaxActive(maxActive);
	}
	public int getMaxIdle() {
		return ds.getMaxIdle();
	}
	public void setMaxIdle(int maxIdle) {
		ds.setMaxIdle(maxIdle);
	}
	public String toString() {
		return ds.toString();
	}
	public int getMinIdle() {
		return ds.getMinIdle();
	}
	public void setMinIdle(int minIdle) {
		ds.setMinIdle(minIdle);
	}
	public int getInitialSize() {
		return ds.getInitialSize();
	}
	public void setInitialSize(int initialSize) {
		ds.setInitialSize(initialSize);
	}
	public long getMaxWait() {
		return ds.getMaxWait();
	}
	public void setMaxWait(long maxWait) {
		ds.setMaxWait(maxWait);
	}
	public boolean isPoolPreparedStatements() {
		return ds.isPoolPreparedStatements();
	}
	public void setPoolPreparedStatements(boolean poolingStatements) {
		ds.setPoolPreparedStatements(poolingStatements);
	}
	public int getMaxOpenPreparedStatements() {
		return ds.getMaxOpenPreparedStatements();
	}
	public void setMaxOpenPreparedStatements(int maxOpenStatements) {
		ds.setMaxOpenPreparedStatements(maxOpenStatements);
	}
	public boolean getTestOnBorrow() {
		return ds.getTestOnBorrow();
	}
	public void setTestOnBorrow(boolean testOnBorrow) {
		ds.setTestOnBorrow(testOnBorrow);
	}
	public boolean getTestOnReturn() {
		return ds.getTestOnReturn();
	}
	public void setTestOnReturn(boolean testOnReturn) {
		ds.setTestOnReturn(testOnReturn);
	}
	public long getTimeBetweenEvictionRunsMillis() {
		return ds.getTimeBetweenEvictionRunsMillis();
	}
	public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
		ds.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
	}
	public int getNumTestsPerEvictionRun() {
		return ds.getNumTestsPerEvictionRun();
	}
	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		ds.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
	}
	public long getMinEvictableIdleTimeMillis() {
		return ds.getMinEvictableIdleTimeMillis();
	}
	public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
		ds.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
	}
	public boolean getTestWhileIdle() {
		return ds.getTestWhileIdle();
	}
	public void setTestWhileIdle(boolean testWhileIdle) {
		ds.setTestWhileIdle(testWhileIdle);
	}
	public int getNumActive() {
		return ds.getNumActive();
	}
	public int getNumIdle() {
		return ds.getNumIdle();
	}
	public String getPassword() {
		return ds.getPassword();
	}
	public void setPassword(String password) {
		ds.setPassword(password);
	}
	public String getUrl() {
		return ds.getUrl();
	}
	public void setUrl(String url) {
		ds.setUrl(url);
	}
	public String getUsername() {
		return ds.getUsername();
	}
	public void setUsername(String username) {
		ds.setUsername(username);
	}
	public String getValidationQuery() {
		return ds.getValidationQuery();
	}
	public void setValidationQuery(String validationQuery) {
		ds.setValidationQuery(validationQuery);
	}
	public boolean isAccessToUnderlyingConnectionAllowed() {
		return ds.isAccessToUnderlyingConnectionAllowed();
	}
	public void setAccessToUnderlyingConnectionAllowed(boolean allow) {
		ds.setAccessToUnderlyingConnectionAllowed(allow);
	}
	public Connection getConnection() throws SQLException {
		return ds.getConnection();
	}
	public Connection getConnection(String username, String password) throws SQLException {
		return ds.getConnection(username, password);
	}
	public int getLoginTimeout() throws SQLException {
		return ds.getLoginTimeout();
	}
	public PrintWriter getLogWriter() throws SQLException {
		return ds.getLogWriter();
	}
	public void setLoginTimeout(int loginTimeout) throws SQLException {
		ds.setLoginTimeout(loginTimeout);
	}
	public void setLogWriter(PrintWriter logWriter) throws SQLException {
		ds.setLogWriter(logWriter);
	}
	public void addConnectionProperty(String name, String value) {
		ds.addConnectionProperty(name, value);
	}
	public void removeConnectionProperty(String name) {
		ds.removeConnectionProperty(name);
	}
	public void close() throws SQLException {
		ds.close();
	}
	public MyDataSource(BasicDataSource ds) {
		super();
		this.ds = ds;
	}

}
