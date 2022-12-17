package rdbms;

import java.util.Map;

public class DbSQLCommand {

	DbEntity _entity;
	String _sql;
	Map<String, Object> _row;
	
	public DbSQLCommand(DbEntity entity, String sql) {
		_entity = entity;
		_sql = sql;
	}

	public String sql() {
		return _sql;
	}

	public void setRow(Map<String, Object> row) {
		_row = row;
	}
	public Map<String, Object> row() {
		return _row;
	}
	public DbEntity entity() {
		return _entity;
	}
}
