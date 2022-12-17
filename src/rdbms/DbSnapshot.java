package rdbms;

import java.util.Map;


public class DbSnapshot {
	long _timestamp;
	Map<String, Object> _snapshot;
	DbEntityID _entityId;
	
	public DbSnapshot(Map<String, Object> row, DbEntityID entityId) {
		this(row, entityId, System.currentTimeMillis());
	}
	public DbSnapshot(Map<String, Object> row, DbEntityID entityId, long timestamp) {
		_timestamp = timestamp;
		_snapshot = row;
		_entityId = entityId;
	}
	public Map<String, Object> snapshot() {
		return _snapshot;
	}
	public void setSnapshot(Map<String, Object> row) {
		_snapshot = row;
	}
	public void setTimestamp(long timestamp) {
		_timestamp = timestamp;
	}
	public DbEntityID entityId() {
		return _entityId;
	}

}
