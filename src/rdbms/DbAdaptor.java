package rdbms;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import rdbms.util.DbLogger;
import ariba.util.core.Assert;
import core.util.ListUtils;
import core.util.MapUtils;

public abstract class DbAdaptor {

	protected DbConnection _connection;
	
	protected DbAdaptor(DbModel model) {
		_connection = new DbConnection(model);
	}
	
	public DbConnection connection() {
		return _connection;
	}
	
	protected DbModel model() {
		return _connection.model();
	}
	
	public abstract boolean supportsSequence();

	public boolean isConnected() {
		return _connection != null && _connection.isConnected();
	}

	public boolean connect() {
		return _connection.establish();
	}

	public void disconnect() {
		_connection.disconnect();
	}
	
	public int executeUpdate(String sqlCommand) throws SQLException {
		Statement stmt = _connection.createStatement();
		int result = -1;
		try {
			result = stmt.executeUpdate(sqlCommand);
		} finally {
			if(stmt != null)	stmt.close();
		}
		return result;
	}
	
	public List<Map<String, Object>> executeQuery(String sqlCommand) throws SQLException {
		List<Map<String, Object>> rows = ListUtils.list();
		
		Statement stmt = _connection.createStatement();
		ResultSet rs = stmt.executeQuery(sqlCommand);
		try {
			while(rs.next()) {
				ResultSetMetaData rsm = rs.getMetaData();
				Map<String, Object> row = MapUtils.map();
				for(int i = 0; i < rsm.getColumnCount(); i++) {
					String colName = rsm.getColumnName(i+1);
					Object value = getData(rs, i+1);
					row.put(colName,  value);
				}
				rows.add(row);
			}
		} finally {
			if(rs != null)		rs.close();
			if(stmt != null)	stmt.close();
		}
		return rows;
	}
	
	public void beginTransaction() throws Exception {
		Assert.that(isConnected(), "Couldn't commit transaction because JDBC Connection is closed");
		_connection.beginTransaction();
	}
	
	public void commitTransaction() throws Exception {
		Assert.that(isConnected(), "Couldn't commit transaction because JDBC Connection is closed");
		_connection.commitTransaction();
	}
	
	public void rollbackTransaction() {
		_connection.rollbackTransaction();
	}
	
	protected Object getData(ResultSet rs, int column) throws SQLException {
		ResultSetMetaData rsm = rs.getMetaData();
		String colName = rsm.getColumnName(column);
		int type = rsm.getColumnType(column);
		Object value = null;
		switch(type) {
		case Types.BIGINT:
			value = rs.getLong(colName);
			break;
		case Types.BIT:
		case Types.BOOLEAN:
			value = rs.getBoolean(colName);
			break;
		case Types.BINARY:
		case Types.BLOB:
			value = rs.getByte(colName);
			break;
		case Types.CHAR:
		case Types.CLOB:
		case Types.VARCHAR:
			value = rs.getString(colName);
			break;
		case Types.DECIMAL:
		case Types.DOUBLE:
			value = rs.getDouble(colName);
			break;
		case Types.FLOAT:
			value = rs.getFloat(colName);
			break;
		case Types.INTEGER:
		case Types.SMALLINT:
			value = new Integer(rs.getInt(colName));
			break;
		case Types.TIME:
		case Types.TIMESTAMP:
		case Types.DATE:
			value = rs.getDate(colName);
			break;
		default:
			value = rs.getString(colName);
			break;
		}
		return value;
	}
	
	protected boolean supportsIsolationLevel(int level) {
		try {
			return _connection.jdbcConnection().getMetaData().supportsTransactionIsolationLevel(level);
		} catch (SQLException e) {
			DbLogger.rdbms_adaptor.warn("Database may not support transaction isolation level function", e);
		}
		return false;
	}
	
	protected int isolationLevel() {
		try {
			return _connection.jdbcConnection().getTransactionIsolation();
		} catch (SQLException e) {
			DbLogger.rdbms_adaptor.warn("Database may not support transaction isolation level function", e);
		}
		return Connection.TRANSACTION_NONE;
	}
	
	protected void setIsolationLevel(int level) {
		try {
			_connection.jdbcConnection().setTransactionIsolation(level);
		} catch (SQLException e) {
			DbLogger.rdbms_adaptor.warn("Database may not support transaction isolation level function", e);
		}
	}
	
	protected abstract int defaultIsolationLevel();
	
	public void lockRecord(DbEntity entity, Object primaryKey) throws SQLException {
		if(supportsIsolationLevel(Connection.TRANSACTION_SERIALIZABLE))
			setIsolationLevel(Connection.TRANSACTION_SERIALIZABLE);
		else if(supportsIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ))
			setIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ);
		else if(supportsIsolationLevel(Connection.TRANSACTION_READ_COMMITTED))
			setIsolationLevel(Connection.TRANSACTION_READ_COMMITTED);
		else
			setIsolationLevel(defaultIsolationLevel());
	}
	
	public void unlockRecord(DbEntity entity, Object primaryKey) {
		setIsolationLevel(defaultIsolationLevel());
	}
	
	public void lockTable(DbEntity entity) throws SQLException {
		if(supportsIsolationLevel(Connection.TRANSACTION_SERIALIZABLE))
			setIsolationLevel(Connection.TRANSACTION_SERIALIZABLE);
		else if(supportsIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ))
			setIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ);
		else if(supportsIsolationLevel(Connection.TRANSACTION_READ_COMMITTED))
			setIsolationLevel(Connection.TRANSACTION_READ_COMMITTED);
		else
			setIsolationLevel(defaultIsolationLevel());
	}
	
	public void unlockTable(DbEntity entity) {
		setIsolationLevel(defaultIsolationLevel());
	}
}
