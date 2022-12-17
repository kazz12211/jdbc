package rdbms;

import java.util.List;
import java.util.Map;

import rdbms.command.AggregateFunctions;
import rdbms.command.Select;
import rdbms.object.DbFaultingList;
import rdbms.util.DbLogger;
import core.util.FieldAccess;
import core.util.ListUtils;
import core.util.MapUtils;
import core.util.Perf;

public class DbQuery extends DbAccess {

	public DbQuery(DbSession session) {
		super(session);
	}

		
	public List loadObjectsInRelationship(Object owner, DbRelationship join) {
		Perf p = Perf.newPerf("Objects in to many join " + join.key() + " loaded");
		List objects = ListUtils.list();
		DbEntity destinationEntity = join.destinationEntity();
		try {
			Object sourceKeyValue = FieldAccess.Util.getValueForKey(owner, join.sourceKey());
			Map<String, Object> fieldValues = MapUtils.map();
			fieldValues.put(join.destinationKey(), sourceKeyValue);
			List values = DbContext.get().executeQuery(destinationEntity.entityClass(), fieldValues);
			if(values != null)
				objects.addAll(values);
		} catch (Exception e) {
			DbLogger.rdbms.error("DbQuery failed to load objects in join '" + join.key() + "'", e);
		}
		p.stop();
		return objects;
	}

	public Object loadObjectInRelationship(Object owner, DbRelationship join) {
		Perf p = Perf.newPerf("Object in to one join " + join.key() + " loaded");
		Object sourceKeyValue = FieldAccess.Util.getValueForKey(owner, join.sourceKey());
		DbEntity destinationEntity = join.destinationEntity();
		Map<String, Object> fieldValues = MapUtils.map();
		fieldValues.put(join.destinationKey(), sourceKeyValue);
		Object value = null;
		if(join.cacheDestination())
			value = session().cachedObject(destinationEntity, join.destinationKey(), sourceKeyValue);
		if(value == null)
			value = DbContext.get().findOne(destinationEntity.entityClass(), fieldValues);
		if(join.cacheDestination())
			session().cache(value, join.destinationKey(), sourceKeyValue);
		p.stop();
		return value;
	}
	
	public Object storedValueForToOneRelationship(Object owner, String key, DbContext context) {
		DbEntity entity = context.entityForObject(owner);
		if(entity == null)
			return null;
		DbRelationship join = entity.relationshipNamed(key);
		if(join == null) {
			DbLogger.rdbms.debug(key + " is not defined in entity " + entity.entityName());
			return null;
		}
		if(join.isToMany()) {
			DbLogger.rdbms.debug("Relationship " + key + " in entity " + entity.entityName() +  " is not to-one");
			return null;
		}
		return loadObjectInRelationship(owner, join);
	}

	public <T> List<T> executeQuery(DbQuerySpecification spec, DbContext DbContext) {
		DbEntity entity = spec.entity();
		List<DbEntity> entities = ListUtils.list();
		if(!entity.isAbstractClass())
			entities.add(entity);
		entities.addAll(entity.concreteDescendantEntities());


		List objects = ListUtils.list();
		for(DbEntity ent : entities) {
			DbQuerySpecification qs;
			DbPredicate newPredicate = null;
			if(ent.inheritance() != null) {
				DbPredicate additionalPredicate = ent.additionalPredicateForInheritance();
				DbPredicate pred = spec.predicate();
				if(pred == null && additionalPredicate != null) {
					newPredicate = additionalPredicate;
				} else if(pred != null && additionalPredicate != null) {
					List<DbPredicate> ps = ListUtils.list();
					ps.add(additionalPredicate);
					if(pred instanceof DbPredicate.And) {
						ps.addAll(((DbPredicate.And) pred).predicates());
					} else {
						ps.add(pred);
					}
					newPredicate = new DbPredicate.And(ps);
				}
			}
			qs = new DbQuerySpecification(ent, newPredicate != null ? newPredicate : spec.predicate());
			qs.setFetchHint(spec.fetchHint());
			qs.setForceRefetch(spec.forceRefetch());
			qs.setSortOrderings(spec.sortOrderings());
			objects.addAll(_executeQuery(qs, DbContext));	
		}
		return objects;
	}

		
	private List<Map<String, Object>> _executeQuery(Select select, DbEntity entity) throws Exception {
		List<Map<String, Object>> results;
		Map<DbEntityID, Map<String, Object>> snapshots = MapUtils.map();
				
		session().establishConnection();
		//boolean forceRefetch = select.querySpecification().forceRefetch();
		/*
		Map<String, Integer> hint = select.querySpecification().fetchHint();
		int fetchLimitSize = (hint != null && hint.get(DbQuerySpecification.FetchLimitSize) != null) ? hint.get(DbQuerySpecification.FetchLimitSize).intValue() : -1;
		int fetchLimitStart = (hint != null && hint.get(DbQuerySpecification.FetchLimitStart) != null)	? hint.get(DbQuerySpecification.FetchLimitStart).intValue() : 0;
			*/
		DbSQLCommand command = select.sqlCommand();
		DbLogger.rdbms_command.debug(command.sql());
		results = session().executeQuery(command);
				
		for(Map<String, Object> row : results) {
			Object pk = entity.primaryKeyForRow(row);
			DbEntityID entityId = session().obtainEntityID(entity, pk);
			snapshots.put(entityId, row);
		}
		session().recordSnapshots(snapshots);
		
		return results;
	}

	public List<Map<String, Object>> rawRowForQuerySpecification(DbQuerySpecification spec, DbContext dbContext) {
		Select select = new Select(spec);
		List<Map<String, Object>> rows = null;
		try {
			rows = this._executeQuery(select, spec.entity());
		} catch (Exception e) {
			DbLogger.rdbms.error("DbQuery failed to fetch", e);
		}
		return rows != null ? rows : ListUtils.EmptyList;
	}
	
	private <T> List<T> _executeQuery(DbQuerySpecification spec, DbContext DbContext) {
		Select select = new Select(spec);
		List<Map<String, Object>> rows = null;
		try {
			rows = this._executeQuery(select, spec.entity());
		} catch (Exception e) {
			DbLogger.rdbms.error("DbQuery failed to fetch", e);
		}
		List list = ListUtils.list();
		if(rows != null) {
			DbEntity entity = spec.entity();
			//DbField pkField = entity.primaryKeyField();
			List<DbField> pkFields = entity.primaryKeyFields();
			for(Map<String, Object> row : rows) {
				Object object = null;
				try {
					DbRowID pk = entity.primaryKeyForRow(row);
					//Object pk = row.get(pkField.columnName());
					DbEntityID entityId = null;
					Object cache = null;
					if(pk != null)
						entityId = session().obtainEntityID(entity, pk);
					if(entityId != null)
						cache = session().cachedObject(entityId);
					if(cache != null && !spec.forceRefetch())
						object = cache;
					else
						object = entity.entityClass().newInstance();
					entity.initObject(object, row, DbContext);
					session().cache(object);
					for(DbRelationship join : entity.relationships()) {
						String key = join.key();
						Object value = null;
						if(join.isToMany()) {
							if(join.shouldPrefetch()) {
								value = this.loadObjectsInRelationship(object, join);
							} else {
								value = new DbFaultingList(object, join);
								((DbFaultingList) value).setFault(true);
							}
						} else {
							if(join.shouldPrefetch())
								value = this.loadObjectInRelationship(object, join);
							else
								value = null;
						}
						FieldAccess.Util.setValueForKey(object, value, key);
					}
					list.add(object);
				} catch (Exception e) {
					DbLogger.rdbms.error("DbQuery: error while initializing object of '" + entity.entityClass().getName() + "' from database row of table '" + entity.entityName() + "'", e);
				}
			}
		}
		
		return list;
		
	}
	

	
	
	// sum, min, max, avg
	public Map<String, Number> executeAggregateFunctions(String key, DbQuerySpecification spec, DbContext DbContext) throws Exception {
		AggregateFunctions functions = new AggregateFunctions(key, spec);
		List<Map<String, Object>> results;
		Map<String, Number> values = MapUtils.map();
		session().establishConnection();
		DbSQLCommand command = functions.sqlCommand();
		results = session().executeQuery(command);
		if(!ListUtils.nullOrEmpty(results)) {
			Map<String, Object> row = results.get(0);
			values.put("count", (Number) row.get("COUNT"));
			values.put("sum", (Number) row.get("SUM"));
			values.put("min", (Number) row.get("MIN"));
			values.put("max", (Number) row.get("MAX"));
			values.put("avg", (Number) row.get("AVG"));
		}
		return values;
	}
		
	public <T> List<T> executeSQL(DbEntity entity, String sqlString, DbRowMapper mapper) throws Exception {
		List<Map<String, Object>> results;
		session().establishConnection();
		DbSQLCommand command = new DbSQLCommand(entity, sqlString);
		DbLogger.rdbms_command.debug("Executing " + command.sql());
		results = session().executeQuery(command);
		List list = ListUtils.list();
		if(!ListUtils.nullOrEmpty(results)) {
			for(Map<String, Object> row : results) {
				DbLogger.rdbms_command.debug("Mapping row " + row);
				Object obj = mapper.createObjectFromRow(row);
				if(obj != null)
				list.add(obj);
			}
		}
		return list;
	}
	
	public <T> T  findOne(DbQuerySpecification spec, DbContext DbContext) {
		List<T> objects = this.executeQuery(spec, DbContext);
		if(objects.size() > 0)
			return objects.get(0);
		return null;
	}
	
	public <T> T  findOne(Class<T> entityClass, Map<String, Object> fieldValues, DbContext DbContext) {
		DbEntity entity = session().model().entityForClass(entityClass);
		DbQuerySpecification spec = new DbQuerySpecification(entity, DbPredicate.Util.createPredicateFromFieldValues(fieldValues));
		return findOne(spec, DbContext);
	}
	
	public <T> T  find(Class<T> entityClass, Object primaryKey, DbContext DbContext) {
		DbEntity entity = session().model().entityForClass(entityClass);
		DbField pkField = entity.primaryKeyField();
		if(pkField == null) {
			DbLogger.rdbms.error("DbQuery: no primary key field in entity '" + entity.entityName() + "'");
			return null;
		}
		Map<String, Object> fieldValues = MapUtils.map();
		fieldValues.put(pkField.key(), primaryKey);
		return findOne(entityClass, fieldValues, DbContext);
	}
	
}
