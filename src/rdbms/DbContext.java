package rdbms;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import rdbms.DbOperation.DatabaseOperationCallback;
import rdbms.util.DbLogger;
import core.util.Assert;
import core.util.ClassUtils;
import core.util.ConservertiveThreadLocal;
import core.util.Delegate;
import core.util.ListUtils;
import core.util.MapUtils;
import core.util.Selector;

public class DbContext implements DatabaseOperationCallback {

	static ConservertiveThreadLocal<DbContext> _threadLocal = new ConservertiveThreadLocal<DbContext>() {
		@Override
		protected DbContext create() {
			return new DbContext();
		}
	};
	
	Map<DbModel, DbSession> _dbSessions = MapUtils.map();
	Map<DbModel, DbQuery> _queryProcessors = MapUtils.map();
	Map<DbModel, DbUpdater> _updateProcessors = MapUtils.map();
	DbContextGroup _contextGroup;
	
	protected DbContext() {
		Map<String, DbModel> models = DbModelGroup.models();
		for(DbModel model : models.values()) {
			DbSession session = new DbSession(model);
			_dbSessions.put(model, session);
			_queryProcessors.put(model, new DbQuery(session));
			_updateProcessors.put(model, new DbUpdater(session));
		}
	}
	
	public static void bind(DbContext context) {
		_threadLocal.set(context);
	}
	public static void unbind() {
		_threadLocal.set(null);
	}
	public static DbContext get() {
		DbContext context = _threadLocal.get(true);
		Assert.that(context != null, "get() on thread local with no DbContext bound");
		return context;
	}
	public static DbContext peek() {
		DbContext context = _threadLocal.get();
		return context;
	}
	public static DbContext createContext() {
		return new DbContext();
	}
	public static void bindNewContext() {
		DbContext context = peek();
		if(context == null)
			bindContext(createContext());
	}
	public static void bindContext(DbContext context) {
		bind(context);
	}
	public <T> T create(Class<T> aClass) {
		T o = (T)ClassUtils.newInstance(aClass);
		Assert.that(o != null,  "Unable to create instance of class: " + aClass.getName());
		this.recordForInsert(o);
		return o;
	}
	public void recordForInsert(Object object) {
		DbEntity entity = entityForObject(object);
		DbUpdater tx = updateProcessorForEntity(entity);
		tx.insert(object, this);
	}
	public void deleteObject(Object object) {
		DbEntity entity = entityForObject(object);
		DbUpdater tx = updateProcessorForEntity(entity);
		tx.delete(object, this);
	}
	public void updateObject(Object object) {
		DbEntity entity = entityForObject(object);
		DbUpdater tx = updateProcessorForEntity(entity);
		tx.update(object, this);
	}
	public DbQuery queryProcessorForEntity(DbEntity entity) {
		return queryProcessorForModel(entity.model());
	}
	public DbQuery queryProcessorForModel(DbModel model) {
		DbQuery query = _queryProcessors.get(model);
		if(query == null) {
			DbSession session = sessionForModel(model);
			query = new DbQuery(session);
			_queryProcessors.put(model, query);
		}
		return query;
	}
	public DbSession sessionForObject(Object object) {
		DbEntity entity = entityForObject(object);
		if(entity != null)
			return sessionForEntity(entity);
		return null;
	}
	public DbSession sessionForEntity(DbEntity entity) {
		DbModel model = entity.model();
		return sessionForModel(model);
	}
	public DbSession sessionForModel(DbModel model) {
		DbSession session = _dbSessions.get(model);
		return session;
	}
	public DbUpdater updateProcessorForEntity(DbEntity entity) {
		return updateProcessorForModel(entity.model());
	}
	public DbUpdater updateProcessorForModel(DbModel model) {
		DbUpdater tx = _updateProcessors.get(model);
		if(tx == null) {
			DbSession session = sessionForModel(model);
			tx = new DbUpdater(session);
			_updateProcessors.put(model, tx);
		}
		return tx;
	}
	public List<?> executeQuery(DbQuerySpecification spec) {
		DbEntity entity = spec.entity();
		if(entity == null)
			entity = entityForClass(spec.entityClass());
		DbQuery query = queryProcessorForEntity(entity);
		return query.executeQuery(spec, this);
	}
	public List<?> executeQuery(Class<?> entityClass, Map<String, Object> fieldValues) {
		DbPredicate predicate = DbPredicate.Util.createPredicateFromFieldValues(fieldValues);
		DbEntity entity = entityForClass(entityClass);
		DbQuerySpecification spec = new DbQuerySpecification(entity, predicate);
		return executeQuery(spec);
	}
	public Object findOne(DbQuerySpecification spec) {
		List<?> results = this.executeQuery(spec);
		if(!ListUtils.nullOrEmpty(results))
			return results.get(0);
		return null;
	}
	public Object findOne(Class<?> entityClass, Map<String, Object> fieldValues) {
		DbPredicate predicate = DbPredicate.Util.createPredicateFromFieldValues(fieldValues);
		DbEntity entity = entityForClass(entityClass);
		DbQuerySpecification spec = new DbQuerySpecification(entity, predicate);
		return findOne(spec);
	}
	
	public Object find(Class<?> entityClass, Object primaryKey) {
		DbEntity entity = entityForClass(entityClass);
		DbQuery query = queryProcessorForEntity(entity);
		return query.find(entityClass, primaryKey, this);
	}
	
	public Object storedValueForToOneRelationship(Object owner, String key) {
		DbEntity entity = entityForObject(owner);
		DbQuery query = queryProcessorForEntity(entity);
		return query.storedValueForToOneRelationship(owner, key, this);
	}
	
	public DbEntity entityForClass(Class<?> entityClass) {
		return DbModelGroup.defaultGroup().entityForClass(entityClass);
	}
	
	public DbEntity entityForObject(Object object) {
		Class<?> entityClass = object.getClass();
		return entityForClass(entityClass);
	}
	public void saveChanges() throws Exception {
		for(DbUpdater tx : _updateProcessors.values()) {
			tx.session().establishConnection();
			if(!tx.hasChanges())	continue;
			tx.begin();
			try {
				tx.executeDatabaseOperations(this);
				tx.commit();
			} catch (DbValidationException invalid) {
				tx.rollback();
				throw invalid;
			} catch (IllegalStateException e) {
				tx.rollback();
				throw e;
			}
			tx.session().disconnect();
		}
	}
	public DbEntityID entityIDForObject(Object object) {
		DbEntity entity = entityForObject(object);
		if(entity != null) {
			DbSession session = sessionForEntity(entity);
			return session.obtainEntityID(entity, entity.primaryKeyForObject(object));
		}
		return null;
	}
	public Object cachedObject(Class<?> entityClass, Object primaryKey) {
		DbEntity entity = entityForClass(entityClass);
		Object object = sessionForEntity(entity).cachedObject(entity, primaryKey);
		if(object == null)
			object = find(entityClass, primaryKey);
		return object;
	}
	public void cache(Object object) {
		DbEntity entity = entityForObject(object);
		sessionForEntity(entity).cache(object);
	}
	public void cacheObjects(Collection<?> objects) {
		for(Object object : objects) {
			DbEntity entity = entityForObject(object);
			sessionForEntity(entity).cache(object);
		}
	}
	public Object reload(Object object) {
		DbEntityID entityId = entityIDForObject(object);
		if(sessionForEntity(entityId.entity()).snapshotForEntityID(entityId) != null)
			sessionForEntity(entityId.entity()).forgetSnapshot(entityId);
		if(objectFromCache(entityId) != null)
			sessionForEntity(entityId.entity()).removeCache(entityId);
		return find(object.getClass(), entityId.entity().primaryKeyForObject(object));
	}
	public Object objectFromCache(DbEntityID entityId) {
		DbEntity entity = entityId.entity();
		DbSession session = sessionForEntity(entity);
		return session.cachedObject(entityId);
	}
	public Object objectFromSnapshot(DbEntityID entityId) throws Exception {
		DbEntity entity = entityId.entity();
		Map<String, Object> snapshot = sessionForEntity(entity).snapshotForEntityID(entityId);
		Object object = ClassUtils.newInstance(entity.entityClass());
		entity.initObject(object, snapshot, this);
		return object;
	}
	@Override
	protected void finalize() throws Throwable {
		closeSessions();
		super.finalize();
	}
	
	private void closeSessions() {
		for(DbSession session : _dbSessions.values()) {
			session.disconnect();
		}
	}
	
	/*
	 * DbDatabaseOperationCallback
	 */
	
	private static Selector awakeFromInsertSelector = new Selector("awakeFromInsert", new Class[]{DbContext.class});
	private static Selector awakeFromFetchSelector = new Selector("awakeFromFetch", new Class[]{DbContext.class});

	@Override
	public void didInsert(Object object) {
		if(Selector.objectRespondsTo(object, awakeFromInsertSelector)) {
			awakeFromInsertSelector.safeInvoke(object, new Object[]{this});
		}
		if(_delegate.respondsTo("awakeFromInsert")) {
			try {
				_delegate.perform("awakeFromInsert", new Object[]{object, this});
			} catch (Exception e) {
				DbLogger.rdbms.error("awakeFromInsert delegate method failed", e);
			}
		}
	}

	@Override
	public void didFetch(Object object) {
		if(Selector.objectRespondsTo(object, awakeFromFetchSelector)) {
			awakeFromFetchSelector.safeInvoke(object, new Object[]{this});
		}
		if(_delegate.respondsTo("awakeFromFetch")) {
			try {
				_delegate.perform("awakeFromFetch", new Object[]{object, this});
			} catch (Exception e) {
				DbLogger.rdbms.error("awakeFromFetch delegate method failed", e);
			}
		}
	}
	
	@Override
	public void didUpdate(Object object, DbEntity entity) {
		if(_delegate.respondsTo("didUpdate")) {
			try {
				_delegate.perform("didUpdate", new Object[]{object, entity, this});
			} catch (Exception e) {
				DbLogger.rdbms.error("didUpdate delegate method failed", e);
			}
		}
	}

	@Override
	public void willUpdate(Object object, DbEntity entity) {
		if(_delegate.respondsTo("willUpdate")) {
			try {
				_delegate.perform("willUpdate", new Object[]{object, entity, this});
			} catch (Exception e) {
				DbLogger.rdbms.error("willUpdate delegate method failed", e);
			}
		}
		
	}

	@Override
	public void didDelete(Object object, DbEntity entity) {
		if(_delegate.respondsTo("didDelete")) {
			try {
				_delegate.perform("didDelete", new Object[]{object, entity, this});
			} catch (Exception e) {
				DbLogger.rdbms.error("didDelete delegate method failed", e);
			}
		}
	}

	@Override
	public void willDelete(Object object, DbEntity entity) {
		if(_delegate.respondsTo("willDelete")) {
			try {
				_delegate.perform("willDelete", new Object[]{object, entity, this});
			} catch (Exception e) {
				DbLogger.rdbms.error("willDelete delegate method failed", e);
			}
		}
	}

	@Override
	public boolean shouldDelete(Object object, DbEntity entity) {
		if(_delegate.respondsTo("shouldDelete")) {
			try {
				return _delegate.booleanPerform("shouldDelete", object, entity, this);
			} catch (Exception e) {
				DbLogger.rdbms.error("willDelete delegate method failed", e);
			}
		}
		return true;
	}
	
	public boolean shouldOrderOperations() {
		if(_delegate.respondsTo("shouldOrderOperations")) {
			try {
				return _delegate.booleanPerform("shouldOrderOperations", this);
			} catch (Exception e) {
				DbLogger.rdbms.error("shouldOrderOperation delegate method failed", e);
			}
		}
		return false;
	}
	public boolean shouldFilterOperations() {
		if(_delegate.respondsTo("shouldFilterOperations")) {
			try {
				return _delegate.booleanPerform("shouldFilterOperations", this);
			} catch (Exception e) {
				DbLogger.rdbms.error("shouldFilterOperations delegate method failed", e);
			}
		}
		return false;
	}
	
	public List<DbOperation> orderOperations(List<DbOperation> operations) {
		if(_delegate.respondsTo("orderOperations")) {
			try {
				return (List<DbOperation>) _delegate.perform("orderOperations", operations, this);
			} catch (Exception e) {
				DbLogger.rdbms.error("orderOperations delegate method failed", e);
			}
		}
		return operations;
	}
	
	public List<DbOperation> filterOperations(List<DbOperation> operations) {
		if(_delegate.respondsTo("orderOperations")) {
			try {
				return (List<DbOperation>) _delegate.perform("filterOperations", operations, this);
			} catch (Exception e) {
				DbLogger.rdbms.error("filterOperations delegate method failed", e);
			}
		}
		return operations;
	}
	
	/*
	 * Delegates
	 */
	
	public interface DbContextDelegate {
		public void awakeFromInsert(Object object, DbContext uniContext);
		public void awakeFromFetch(Object object, DbContext uniContext);
		public void willUpdate(Object object, DbEntity entity, DbContext uniContext);
		public void didUpdate(Object object, DbEntity entity, DbContext uniContext);
		public void willDelete(Object object, DbEntity entity, DbContext uniContext);
		public void didDelete(Object object, DbEntity entity, DbContext uniContext);
		public boolean shouldDelete(Object object, DbEntity entity, DbContext uniContext);
		public boolean shouldOrderOperations(DbContext uniContext);
		public List<DbOperation> orderOperations(List<DbOperation> operations, DbContext uniContext);
		public boolean shouldFilterOperations(DbContext uniContext);
		public List<DbOperation> filterOperations(List<DbOperation> opearations, DbContext uniContext);
	}
	
	private Delegate _delegate = new Delegate(DbContextDelegate.class);
	
	public void setDelegate(Object delegateObject) {
		_delegate.setDelegate(delegateObject);
	}

	public static void bindNewContext(String groupName) {
		bindContext(createContext(), groupName);
	}
	
	static void bindContext(DbContext ctx, String groupName) {
		ctx._contextGroup = associateContextGroup(ctx, groupName);
		bind(ctx);
	}
	
	static Map<String, DbContextGroup> _ContextGroups = MapUtils.map();
	
	static DbContextGroup associateContextGroup(DbContext ctx, String name) {
		gcEmptyContextGroups();
		
		if(name == null)
			return new DbContextGroup(null);
			
		synchronized(_ContextGroups) {
			DbContextGroup group = _ContextGroups.get(name);
			if(group == null) {
				group = new DbContextGroup(name);
				_ContextGroups.put(name, group);
			}
			group.registerMember(ctx);
			return group;
		}
	}
	
	static void gcEmptyContextGroups() {
		synchronized(_ContextGroups) {
			Iterator <Map.Entry<String, DbContextGroup>> iter = _ContextGroups.entrySet().iterator();
			while(iter.hasNext()) {
				Map.Entry<String, DbContextGroup> e = iter.next();
				if(e.getValue()._members.size() == 0)
					iter.remove();
			}
		}
	}
	
	static class DbContextGroup {
		String _name;
		int _saveCount;
		WeakHashMap<DbContext, Object> _members = new WeakHashMap();
		
		DbContextGroup(String name) {
			_name = name;
		}
		void registerMember(DbContext member) {
			_members.put(member, true);
		}
		Iterator <DbContext> memberIterator() {
			return _members.keySet().iterator();
		}
	}
	
	public void remove(Object object) {
		DbEntity entity = entityForObject(object);
		DbUpdater tx = updateProcessorForEntity(entity);
		tx.remove(object, this);
	}
}
