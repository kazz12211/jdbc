package rdbms;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import rdbms.util.DbLogger;
import core.util.ListUtils;

public abstract class DbPrimaryKeyGenerator {

	Map<String, Object> _config;

	public Object newPrimaryKeyForObject(Object object, DbContext DbContext) {
		DbEntity entity = DbContext.entityForObject(object);
		if(entity == null)
			return null;
		return newPrimaryKeyForEntity(entity, DbContext);
	}
	
	public abstract Object newPrimaryKeyForEntity(DbEntity entity, DbContext DbContext);
	
	public void setConfig(Map<String, Object> dict) {
		this._config = dict;
	}
	public Map<String, Object> config() {
		return _config;
	}
	
	public static class SequenceTableKeyGenerator extends DbPrimaryKeyGenerator {

		public static final String PK_TABLE_NAME = "_SEQUENCE_TABLE";

		@Override
		public Object newPrimaryKeyForEntity(DbEntity entity,
				DbContext context) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	public static class SequenceKeyGenerator extends DbPrimaryKeyGenerator {

		private String _sequenceName;

		public SequenceKeyGenerator(String sequenceName) {
			_sequenceName = sequenceName;
		}
		
		private String sequenceSQL(DbEntity entity) {
			DbField field = entity.primaryKeyField();
			if(field != null && _sequenceName != null)
				return "select nextval('" + _sequenceName + "')";
			return null;
		}
		
		
		@Override
		public Object newPrimaryKeyForEntity(DbEntity entity,
				DbContext context) {
			Object pk = null;
			List<Map<String, Object>> rows = null;
			try {
				rows = context.sessionForEntity(entity).executeQuery(new DbSQLCommand(entity, sequenceSQL(entity)));
			} catch (SQLException e) {
				DbLogger.rdbms.error("Couldn't obtain sequence for entity " + entity.entityName(), e);
			}
			if(!ListUtils.nullOrEmpty(rows)) {
				Map<String, Object> row = rows.get(0);
				pk = row.values().iterator().next();
			}
		
			return pk;
		}
		
	}

}
