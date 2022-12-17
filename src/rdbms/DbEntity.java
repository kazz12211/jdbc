package rdbms;

import java.lang.reflect.Modifier;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import rdbms.util.DbLogger;
import core.util.Accessor;
import core.util.FieldAccess;
import core.util.ListUtils;
import core.util.MapUtils;

public class DbEntity {

	public static final String OptimisticLock = "optimistic";
	public static final String PessimisticLock = "pessimistic";
	public static final String FileLock = "file";
	public static final String NoCache ="none";
	public static final String NormalCache = "normal";
	public static final String StatisticalCache = "statistical";
	public static final String DistantFutureCache = "distantfuture";
	
	public enum CacheStrategy {
		None, Normal, Statistical, DistantFuture
	}
	
	public enum LockingStrategy {
		Optimistic, Pessimistic, File
	}

	DbModel _model;
	String _tableName;
	String _entityName;
	Class<?> _entityClass;
	DbField[] _fields;
	DbRelationship[] _relationships;
	DbPrimaryKeyGenerator _pkGenerator;
	LockingStrategy _lockingStrategy = LockingStrategy.Optimistic;
	CacheStrategy _cacheStrategy = CacheStrategy.Normal;
	List<String> _lockKeys;
	List<String> _lockColumnNames;
	DbInheritance _inheritance;
	boolean _compositePrimaryKey;
	
	public DbEntity(DbModel model, Class<?> entityClass, String entityName, String tableName, DbField[] fields) {
		this._entityClass = entityClass;
		this._entityName = entityName;
		this._tableName = tableName;
		this._model = model;
		this._fields = fields;
	}
	
	public DbEntity(DbModel model, Class<?> entityClass, String entityName, String tableName) {
		this._entityClass = entityClass;
		this._entityName = entityName;
		this._tableName = tableName;
		this._model = model;
	}

	public void setModel(DbModel model) {
		this._model = model;
	}
	public DbModel model() {
		return _model;
	}
	public Object primaryKeyForObject(Object object) {
		List<DbField> pkFields = primaryKeyFields();
		List<String> keys = ListUtils.list();
		List<Object> values = ListUtils.list();
		for(DbField field : pkFields) {
			keys.add(field.key());
			values.add(FieldAccess.Util.getValueForKey(object, field.key()));
		}
		DbRowID rowId = new DbRowID(keys, values);
		/*
		DbField pkField = primaryKeyField();
		if(pkField != null) {
			return FieldAccess.Util.getValueForKey(object, pkField.key());
		}
		*/
		return rowId;
	}
	
	public void setPrimaryKeyForObject(Object pk, Object object) {
		DbRowID rowId = (DbRowID) pk;
		Map<String, Object> ids = rowId.ids();
		for(String key : ids.keySet()) {
			FieldAccess.Util.setValueForKey(object, ids.get(key), key);
		}
		/*
		DbField pkField = primaryKeyField();
		if(pkField != null) {
			 FieldAccess.Util.setValueForKey(object, pk, pkField.key());
		}
		*/
	}
	
	public String entityName() {
		return _entityName;
	}
	public Class<?> entityClass() {
		return _entityClass;
	}
	
	public DbField[] fields() {
		return _fields;
	}
		
	public String tableName() {
		return _tableName;
	}
	
	public DbRelationship[] relationships() {
		return _relationships;
	}
	
	public LockingStrategy lockingStrategy() {
		return _lockingStrategy;
	}
	
	public CacheStrategy cacheStrategy() {
		return _cacheStrategy;
	}
	

	public DbField primaryKeyField() {
		for(DbField field : fields()) {
			if(field._isPrimaryKey)
				return field;
		}
		return null;
	}
	public List<DbField> primaryKeyFields() {
		List<DbField> list = ListUtils.list();
		for(DbField field : fields()) {
			if(field._isPrimaryKey)
				list.add(field);
		}
		return list;
	}
	
	public DbField fieldNamed(String fieldName) {
		for(DbField field : fields()) {
			if(field.key().equals(fieldName))
				return field;
		}
		return null;
	}
	
	public DbField fieldWithColumnName(String columnName) {
		for(DbField field : fields()) {
			if(field.columnName().equals(columnName))
				return field;
		}
		return null;
	}
	
	public DbRelationship relationshipNamed(String key) {
		for(DbRelationship join : relationships()) {
			if(join._key.equals(key))
				return join;
		}
		return null;
	}

	public DbInheritance inheritance() {
		return _inheritance;
	}
	
	public DbEntity parentEntity() {
		if(_inheritance != null)
			return _inheritance.parentEntity();
		return null;
	}
	
	public List<DbEntity> childEntities() {
		List<DbEntity> subs = new ArrayList<DbEntity>();
		for(DbEntity entity : _model.entities()) {
			if(entity == this) continue;
			if(entity.parentEntity() == this && !subs.contains(entity)) {
				subs.add(entity);
				subs.addAll(entity.childEntities());
			}
		}
		return subs;
	}
	
	public void initObject(Object object, Map<String, Object> row, DbContext context) throws Exception {
		for(DbField field : fields()) {
			String columnName = field.columnName();
			String key = field.key();
			Object value = row.get(columnName);
			if(Accessor.newSetAccessor(object.getClass(), key) == null) {
				DbLogger.rdbms.warn("Instance of entity '" + field.entity().entityName() + "' does not have property '" + key + "'");
				continue;
			}
			try {
				FieldAccess.Util.setValueForKey(object, field.coerceValue(value), key);
			} catch (Exception e) {
				DbLogger.rdbms.error("DbEntity.initObject(): could not set value '" + value + "' to field '" + key + "'.\n value is " + 
			(value != null ? value.getClass().getName() : "null") + " expected value is " + field.valueClass().getName(), e);
				throw e;
			}
		}
		
		context.didFetch(object);
	}
		

	@Override
	public String toString() {
		return "{entityName=" + _entityName + "; filename=" + _tableName + "; entityClass=" + _entityClass.getName() + "; fields=" + fieldsDescription() + "; inheritance=" + _inheritance + "}";
	}

	private String fieldsDescription() {
		List<String> descs = new ArrayList<String>();
		for(DbField field : fields()) {
			descs.add(field.toString());
		}
		return "(" + ListUtils.listToString(descs, ", ") + ")";
	}
	
	public DbPrimaryKeyGenerator pkGenerator() {
		return _pkGenerator;
	}
	public void setPkGenerator(DbPrimaryKeyGenerator pkGenerator) {
		this._pkGenerator = pkGenerator;
	}
		
	public List<String> lockColumnNames() {
		if(_lockColumnNames == null) {
			_lockColumnNames = ListUtils.list();
			for(DbField field : fields()) {
				if(field.isLockKey())
					_lockColumnNames.add(field.columnName());
			}
		}
		return _lockColumnNames;
	}
	public List<String> lockKeys() {
		if(_lockKeys == null) {
			_lockKeys = ListUtils.list();
			for(DbField field : fields()) {
				if(field.isLockKey())
					_lockKeys.add(field.key());
			}
		}
		return _lockKeys;
	}
	
	public boolean isAbstractClass() {
		return (_entityClass.getModifiers() & Modifier.ABSTRACT) != 0;
	}

	public List<? extends DbEntity> concreteDescendantEntities() {
		List<DbEntity> descendants = new ArrayList<DbEntity>();
		if(this.childEntities().size() > 0) {
			for(DbEntity child : this.childEntities()) {
				if(!child.isAbstractClass())
					descendants.add(child);
				descendants.addAll(child.concreteDescendantEntities());
			}
		}
		return descendants;
	}

	public DbPredicate additionalPredicateForInheritance() {
		return _inheritance.predicate();
	}

	public DbSQLCommand createUpdateCommand(Object object, DbContext context) {
		List<String> columnNames = ListUtils.list();
		List<String> values = ListUtils.list();
		
		Map<String, Object> row = MapUtils.map();
		for(DbField field : _fields) {
			if(field.isPrimaryKey())	continue;
			columnNames.add(field.columnName());
			Object val = FieldAccess.Util.getValueForKey(object, field.key());
			row.put(field.columnName(), val);
			values.add(stringValue(val));
		}
		
		StringBuffer sql = new StringBuffer();
		sql.append("UPDATE " + this._tableName + " SET ");
		boolean start = true;
		for(int i = 0, size = columnNames.size(); i < size; i++) {
			if(start)	start = false;
			else		sql.append(",");
			String cn = columnNames.get(i);
			String val = values.get(i);
			sql.append(cn + "=" + val);
		}
		sql.append(" WHERE ");
		if(lockingStrategy() == DbEntity.LockingStrategy.Optimistic) {
			List<String> locks = ListUtils.list();
			locks.add(primaryKeyField().columnName() + "=" + stringValue(primaryKeyForObject(object)));
			for(int i = 0, size = lockKeys().size(); i < size; i++) {
				locks.add(_lockColumnNames.get(i) + "=" + stringValue(FieldAccess.Util.getValueForKey(object, _lockKeys.get(i))));
			}
			sql.append(ListUtils.listToString(locks, " AND "));
		} else {
			sql.append(primaryKeyField().columnName() + "=" + stringValue(primaryKeyForObject(object)));
		}
		DbSQLCommand command = new DbSQLCommand(this, sql.toString());
		command.setRow(row);
		return command;
	}

	public DbSQLCommand createDeleteCommand(Object object, DbContext context) {
		StringBuffer sql = new StringBuffer();
		sql.append("DELETE FROM " + this.tableName() + " WHERE ");
		if(lockingStrategy() == DbEntity.LockingStrategy.Optimistic) {
			List<String> locks = ListUtils.list();
			locks.add(primaryKeyField().columnName() + "=" + stringValue(primaryKeyForObject(object)));
			for(int i = 0, size = lockKeys().size(); i < size; i++) {
				locks.add(_lockColumnNames.get(i) + "=" + stringValue(FieldAccess.Util.getValueForKey(object, _lockKeys.get(i))));
			}
			sql.append(ListUtils.listToString(locks, " AND "));
		} else {
			sql.append(primaryKeyField().columnName() + "=" + stringValue(primaryKeyForObject(object)));
		}
		DbSQLCommand command = new DbSQLCommand(this, sql.toString());
		return command;
	}

	public DbSQLCommand createInsertCommand(Object object, DbContext context) {
		List<String> columnNames = ListUtils.list();
		List<String> values = ListUtils.list();
		Map<String, Object> row = MapUtils.map();
		for(DbField field : _fields) {
			if(field.isReadOnly())	continue;
			String columnName = field.columnName();
			Object val = FieldAccess.Util.getValueForKey(object, field.key());
			row.put(columnName, val);
			String value = stringValue(val);
			if(value != null) {
				columnNames.add(columnName);
				values.add(value);
			}
		}
		
		StringBuffer sql = new StringBuffer();
		sql.append("INSERT INTO " + tableName() + " (");
		boolean start = true;
		for(String cn : columnNames) {
			if(start)	start = false;
			else		sql.append(",");
			sql.append(cn);
		}
		sql.append(") values (");
		start = false;
		for(String val : values) {
			if(start)	start = false;
			else		sql.append(",");
			sql.append(val);
		}
		sql.append(")");
		DbSQLCommand command = new DbSQLCommand(this, sql.toString());
		command.setRow(row);
		return command;
	}
	
	private static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private String stringValue(Object value) {
		if(value == null)
			return "NULL";
		if(value instanceof Date) {
			return "'" + DATE_FORMAT.format((Date) value) + "'";
		} else if(value instanceof String) {
			return "'" + value + "'";
		} else if(value instanceof Boolean) {
			return ((Boolean) value).booleanValue() ? "true" : "false";
		} else {
			return value.toString();
		}
	}

	public String columnNames() {
		List<String> names = ListUtils.list();
		for(DbField field : _fields) {
			names.add(field.columnName());
		}
		return ListUtils.listToString(names, ", ");
	}

	public DbRowID primaryKeyForRow(Map<String, Object> row) {
		List<DbField> fields = this.primaryKeyFields();
		List<String> keys = ListUtils.list();
		List<Object> values = ListUtils.list();
		for(DbField field : fields) {
			keys.add(field.key());
			values.add(row.get(field.columnName()));
		}
		return new DbRowID(keys, values);
	}


}
