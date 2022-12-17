package rdbms;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import rdbms.util.DbLogger;
import core.util.ClassUtils;
import core.util.ListUtils;
import core.util.MapUtils;
import core.util.Perf;
import core.util.StringUtils;

public class DbSession {
	protected DbAdaptor _adaptor;
	protected DbModel _model;
	private Map<DbEntityID, DbSnapshot> _snapshots;
	private List<DbEntityID> _entityIds;
	protected DbEntityCache _entityCache;

	private static Map<String, String> _adaptorClassNames = null;
	static {
		_adaptorClassNames = MapUtils.map();
		_adaptorClassNames.put("org.postgresql.Driver", "rdbms.adaptor.PGSQLAdaptor");
		_adaptorClassNames.put("com.mysql.jdbc.Driver", "rdbms.adaptor.MySQLAdaptor");
	}
	
	public DbSession(DbModel model) {
		_model = model;
		_adaptor = createAdaptor();
	}

	private DbAdaptor createAdaptor() {
		DbConnectionInfo info = _model.connectionInfo();
		String driverClass = info.driverClass();
		if(StringUtils.nullOrEmptyOrBlank(driverClass))
			return null;
		String adaptorClass = _adaptorClassNames.get(driverClass);
		if(StringUtils.nullOrEmptyOrBlank(adaptorClass))
			return null;
		Class<?> aClass = ClassUtils.classForName(adaptorClass, DbAdaptor.class);
		if(aClass == null)
			return null;
		Constructor<?> c = null;
		try {
			c = aClass.getConstructor(DbModel.class);
		} catch (Exception e) {
			DbLogger.rdbms_adaptor.error("Can not instantiate adaptor " + adaptorClass + ". No valid constructor defined.");
		}
		if(c != null)
			try {
				return (DbAdaptor) c.newInstance(_model);
			} catch (Exception e) {
				DbLogger.rdbms_adaptor.error("Could not instantiate adaptor " + adaptorClass + ".");
			}
		
		return null;
	}

	public DbModel model() {
		return _model;
	}
	
	public DbAdaptor adaptor() {
		return _adaptor;
	}
	
	public void establishConnection() throws Exception {
		boolean success = _adaptor.isConnected();
		
		if(!success) {
			success = _adaptor.connect();
		}
		if(success) {
			if(_snapshots == null)
			_snapshots = MapUtils.map();
			if(_entityIds == null)
			_entityIds = ListUtils.list();
			if(_entityCache == null)
			_entityCache = new DbEntityCache();
		}
	}
	
	public void disconnect() {
		if(_adaptor.isConnected()) {
			_adaptor.disconnect();
			_snapshots.clear();
			_snapshots = null;
			_entityIds.clear();
			_entityIds = null;
			_entityCache.clear();
		}
	}
	
	public void recordSnapshot(DbEntityID entityId, Map<String, Object> row) {
		DbLogger.rdbms_snapshot.debug("[SNAPSHOT] Recording snapshot: entityId=" + entityId.toString() + " snapshot=" + row);
		DbSnapshot snapshot = new DbSnapshot(row, entityId);
		_snapshots.put(entityId, snapshot);
	}
	
	public void updateSnapshot(DbEntityID entityId, Map<String, Object> row) {
		DbLogger.rdbms_snapshot.debug("[SNAPSHOT] Updating snapshot: entityId=" + entityId.toString() + " snapshot=" + row);
		DbSnapshot snapshot = _snapshots.get(entityId);
		if(snapshot != null) {
			snapshot.setSnapshot(row);
			snapshot.setTimestamp(System.currentTimeMillis());
		}
	}

	public void forgetSnapshot(DbEntityID entityId) {
		DbLogger.rdbms_snapshot.debug("[SNAPSHOT] Forgetting snapshot: entityId=" + entityId.toString());
		DbSnapshot snapshot = _snapshots.get(entityId);
		if(snapshot != null) {
			_snapshots.remove(entityId);
			snapshot = null;
		}
	}
	
	public Map<String, Object> snapshotForEntityID(DbEntityID entityId) {
		DbSnapshot snapshot = _snapshots.get(entityId);
		if(snapshot == null)
			return null;
		DbLogger.rdbms_snapshot.debug("[SNAPSHOT] Load snapshot: entityId=" + entityId.toString() + " snapshot=" + snapshot.snapshot());
		return snapshot.snapshot();
	}
	
	public DbSnapshot snapshotForPredicate(DbEntity entity, DbPredicate predicate) {
		for(DbSnapshot r : _snapshots.values()) {
			if(r._entityId.entity().equals(entity)) {
				if(predicate.matchesToRow(r._snapshot, entity))
					return r;
			}
		}
		return null;
	}
	
	public DbSnapshot snapshotForFieldValues(DbEntity entity, Map<String, Object> fieldValues) {
		DbPredicate p = DbPredicate.Util.createPredicateFromFieldValues(fieldValues);
		return snapshotForPredicate(entity, p);
	}
	
	public void recordSnapshots(Map<DbEntityID, Map<String, Object>> snapshots) {
		for(DbEntityID entityId : snapshots.keySet()) {
			recordSnapshot(entityId, snapshots.get(entityId));
		}
	}
	
	public DbEntityID obtainEntityID(Object object) {
		if(object == null)
			return null;
		DbEntity entity = DbModelGroup.defaultGroup().entityForClass(object.getClass());
		Object primaryKey = entity.primaryKeyForObject(object);
		return obtainEntityID(entity, primaryKey);
	}
	
	public DbEntityID obtainEntityID(DbEntity entity, Object primaryKey) {
		for(DbEntityID entityId : _entityIds) {
			if(entityId instanceof DbEntityID.PK && entityId.entity().equals(entity) && ((DbEntityID.PK) entityId).primaryKey().equals(primaryKey))
				return entityId;
		}
		DbEntityID newEntityId = new DbEntityID.PK(entity, primaryKey);
		_entityIds.add(newEntityId);
		return newEntityId;
	}
	
	public DbEntityID obtainEntityID(DbEntity entity, String key, Object value) {
		for(DbEntityID entityId : _entityIds) {
			if(entityId instanceof DbEntityID.KeyValue && 
					entityId.entity().equals(entity) && 
					((DbEntityID.KeyValue) entityId).key().equals(key) &&
					((DbEntityID.KeyValue) entityId).value().equals(value))
				return entityId;
		}
		DbEntityID newEntityId = new DbEntityID.KeyValue(entity, key, value);
		_entityIds.add(newEntityId);
		return newEntityId;
	}
	
	public DbEntityID lookupEntityIDInCache(DbEntity entity, Object primaryKey) {
		DbEntityID entityId = this.obtainEntityID(entity, primaryKey);
		return _entityCache.contains(entityId) ? entityId : null;
	}
	
	public Object cachedObject(DbEntity entity, Object primaryKey) {
		DbEntityID entityId = this.obtainEntityID(entity, primaryKey);
		return cachedObject(entityId);
	}
	
	public Object cachedObject(DbEntityID entityId) {
		Object object = _entityCache.get(entityId);
		return object;
	}
	public Object cachedObject(DbEntity entity, String key, Object value) {
		DbEntityID entityId = this.obtainEntityID(entity, key, value);
		return cachedObject(entityId);
	}
	
	public void cache(Object object) {
		if(object != null) {
			DbEntityID entityId = this.obtainEntityID(object);
			_entityCache.add(entityId, object);
		}
	}
	
	public void cache(Object object, String key, Object value) {
		if(object != null) {
			DbEntity entity = DbModelGroup.defaultGroup().entityForClass(object.getClass());
			DbEntityID entityId = this.obtainEntityID(entity, key, value);
			_entityCache.add(entityId, object);
		}
	}


	public void cacheObjects(Collection<?> objects) {
		for(Object object : objects)
			this.cache(object);
	}
	
	public void clearCache() {
		_entityCache.clear();
	}
	
	public void removeCache(DbEntityID entityId) {
		_entityCache.remove(entityId);
	}
	
	public int executeUpdate(DbSQLCommand command) throws SQLException {
		return _adaptor.executeUpdate(command.sql());
	}

	public List<Map<String, Object>> executeQuery(DbSQLCommand command) throws SQLException {
		Perf p = Perf.newPerf("Adaptor " + _adaptor.toString() + " got the result of SQL \"" + command.sql() + "\"");
		List<Map<String, Object>> rows = _adaptor.executeQuery(command.sql());
		p.stop();
		return rows;
	}

	public void beginTransaction() throws Exception {
		_adaptor.beginTransaction();
	}
	
	public void commitTransaction() throws Exception {
		_adaptor.commitTransaction();
	}
	
	public void rollbackTransaction() {
		_adaptor.rollbackTransaction();
	}


}
