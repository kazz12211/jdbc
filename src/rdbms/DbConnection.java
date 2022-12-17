package rdbms;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import core.util.ClassUtils;
import rdbms.util.DbLogger;

public class DbConnection {

	private static final int CONN_TIMEOUT_SECS = 5;
	
	Connection _connection; //JDBC connection
	DbModel _model;
	
	public DbConnection(DbModel model) {
		_model = model;
	}

	public DbModel model() {
		return _model;
	}

	public boolean isConnected() {
		if(_connection == null)
			return false;
		/*
		boolean valid = false;
		try {
			valid = _connection.isValid(CONN_TIMEOUT_SECS);
		} catch (SQLException e) {
			DbLogger.rdbms.error("JDBC connection seems to be closed", e);
		}
		return valid;
		*/
		return true;
	}

	public boolean establish() {
		DbConnectionInfo connInfo = _model.connectionInfo();
		ClassUtils.classForName(connInfo.driverClass());
		try {
			_connection = DriverManager.getConnection(connInfo.url(), connInfo.username(), connInfo.password());
		} catch (SQLException e) {
			DbLogger.rdbms.error("Couldn't open JDBC connection", e);
		}
		return isConnected();
	}

	public void disconnect() {
		if(_connection != null) {
			try {
				_connection.close();
			} catch (SQLException e) {
				DbLogger.rdbms.warn("Failed to close JDBC connection", e);
			}
		}
		
	}

	public void beginTransaction() throws SQLException {
		if(_connection != null) {
			_connection.setAutoCommit(false);
		}
	}

	public void commitTransaction() throws SQLException {
		if(_connection != null) {
			_connection.commit();
		}
	}
	
	public void rollbackTransaction() {
		if(_connection != null) {
			try {
				_connection.rollback();
			} catch (SQLException e) {
				DbLogger.rdbms.warn("Failed to rollback JDBC transaction", e);
			}
		}
	}

	public Statement createStatement() throws SQLException {
		return _connection.createStatement();
	}

	public Connection jdbcConnection() {
		return _connection;
	}


}
