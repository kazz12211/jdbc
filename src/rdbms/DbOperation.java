package rdbms;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import rdbms.util.DbLogger;
import core.util.FieldAccess;
import core.util.ListUtils;
import core.util.Perf;

public abstract class DbOperation {

	Object _object;
	DbEntity _entity;
	long _millis;
	
	protected DbOperation(Object object, DbEntity entity) {
		_object = object;
		_entity = entity;
		_millis = System.currentTimeMillis();
	}
	
	public long timestamp() {
		return _millis;
	}
	public Object object() {
		return _object;
	}
	public DbEntity entity() {
		return _entity;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " {object=" + _object + "; entity=" + _entity.entityName() + "; timestamp=" + new Date(_millis) + "}";
	}

	public abstract void executeInContext(DbContext dbContext) throws Exception;
	
	public DbUpdater getProcessorInContext(DbContext DbContext) {
		DbUpdater updater = DbContext.updateProcessorForEntity(_entity);
		return updater;
	}
	
	public static List<DbOperation> sortedOperations(List<DbOperation> operations) {
		int size = operations.size();
		List<DbOperation> copy = ListUtils.list(size);
		for(int i = 0; i < size; i++) {
			copy.add(operations.get(i));
		}
		Collections.sort(copy, new Comparator<DbOperation>() {
			@Override
			public int compare(DbOperation arg0,
					DbOperation arg1) {
				return arg0._millis - arg1._millis > 0 ? 1 : arg0._millis - arg1._millis < 0 ? -1 : 0;
			}});
		return copy;
	}
	
	
	public interface DatabaseOperationCallback {
		public void didInsert(Object object);
		public void didFetch(Object object);
		public void didUpdate(Object object, DbEntity entity);
		public void willUpdate(Object object, DbEntity entity);
		public void didDelete(Object object, DbEntity entity);
		public void willDelete(Object object, DbEntity entity);
		public boolean shouldDelete(Object object, DbEntity entity);
	}
	
	public static class Update extends DbOperation {

		public Update(Object object, DbEntity entity) {
			super(object, entity);
		}

		@Override
		public void executeInContext(DbContext context) throws Exception {
			Object pk = _entity.primaryKeyForObject(_object);
						
			DbUpdater updater = this.getProcessorInContext(context);			

			context.willUpdate(_object, _entity);
			
			Perf p = Perf.newPerf(_entity.entityName() + "(" + pk + ") updated");
			this.obtainLock(pk, updater);
			
			DbSQLCommand updateCommand = _entity.createUpdateCommand(_object, context);
			updater.session().executeUpdate(updateCommand);
			Map<String, Object> row = updateCommand.row();

			this.releaseLock(pk, updater);
			p.stop();
			
			context.didUpdate(_object, _entity);

			
			DbEntityID entityId = updater.session().obtainEntityID(_entity, _entity.primaryKeyForObject(_object));
			if(entityId != null && updater != null) {
				updater.session().updateSnapshot(entityId, row);
			}
		}

		private void obtainLock(Object pk, DbUpdater updater) throws Exception {
			if(_entity.lockingStrategy() == DbEntity.LockingStrategy.Pessimistic) {
				updater.session().adaptor().lockRecord(_entity, pk);
			} else if(_entity.lockingStrategy() == DbEntity.LockingStrategy.File) {
				updater.session().adaptor().lockTable(_entity);
			}
		}
		
		private void releaseLock(Object pk, DbUpdater updater) {
			if(_entity.lockingStrategy() == DbEntity.LockingStrategy.Pessimistic) {
				updater.session().adaptor().unlockRecord(_entity, pk);
			} else if(_entity.lockingStrategy() == DbEntity.LockingStrategy.File) {
				updater.session().adaptor().unlockTable(_entity);
			}
		}

	}

	public static class Delete extends DbOperation {

		public Delete(Object object, DbEntity entity) {
			super(object, entity);
		}
		
		private void deleteDestination(Object object, DbEntity entity, DbUpdater updater, DbContext context) throws Exception {
			context.willDelete(object, entity);
			DbSQLCommand deleteCommand = entity.createDeleteCommand(object, context);
			updater.session().executeUpdate(deleteCommand);
			context.didDelete(object, entity);
		}

		private void deleteDestinations(Collection<?> objects, DbEntity entity, DbUpdater updater, DbContext context) throws Exception {
			for(Object object : objects) {
				deleteDestination(object, entity, updater, context);
			}
		}
		private void nullifyDestination(Object object, DbEntity entity, String sourceKey, DbUpdater updater, DbContext context) throws Exception {
			FieldAccess.Util.setValueForKey(object, null, sourceKey);
			DbSQLCommand updateCommand = entity.createUpdateCommand(object, context);
			context.willUpdate(object, entity);
			updater.session().executeUpdate(updateCommand);
			context.didUpdate(object, entity);
		}
		
		private void nullifyDestinations(Collection<?> objects, DbEntity entity, String sourceKey, DbUpdater updater, DbContext context) throws Exception {
			for(Object object : objects) {
				nullifyDestination(object, entity, sourceKey, updater, context);
			}
		}
		
		@Override
		public void executeInContext(DbContext context) throws Exception {
			if(!context.shouldDelete(_object, _entity))
				return;
			
			context.willDelete(_object, _entity);

			DbUpdater updater = this.getProcessorInContext(context);			
			Object pk = _entity.primaryKeyForObject(_object);
			Perf p = Perf.newPerf(_entity.entityName() + "(" + pk + ") deleted");
			DbSQLCommand deleteCommand = _entity.createDeleteCommand(_object, context);
			updater.session().executeUpdate(deleteCommand);
			
			DbRelationship relationships[] = _entity.relationships();
			if(relationships != null & relationships.length > 0) {
				for(DbRelationship relationship : relationships) {
					Object value = FieldAccess.Util.getValueForKey(_object, relationship.key());
					
					if(value == null)
						continue;
					if(relationship.ownsDestination()) {
						if(relationship.isToMany() && value instanceof Collection) {
							deleteDestinations((Collection<?>)value, relationship.destinationEntity(), updater, context);
						} else {
							deleteDestination(value, relationship.destinationEntity(), updater, context);
						}
					} else {
						if(relationship.isToMany() && value instanceof Collection) {
							nullifyDestinations((Collection<?>)value, relationship.destinationEntity(), relationship.sourceKey(), updater, context);
						} else {
							nullifyDestination(value, relationship.destinationEntity(), relationship.sourceKey(), updater, context);
						}
					}
				}
			}
			
			p.stop();
			
			context.didDelete(_object, _entity);
			
			DbEntityID entityId = updater.session().obtainEntityID(_entity, _entity.primaryKeyForObject(_object));
			if(entityId != null && updater != null)
				updater.session().forgetSnapshot(entityId);
		}

	}

	public static class Insert extends DbOperation {

		public Insert(Object object, DbEntity entity) {
			super(object, entity);
		}

		@Override
		public void executeInContext(DbContext context) throws Exception {
			Object pk = _entity.primaryKeyForObject(_object);
			if(pk == null) {
				Perf p = Perf.newPerf("Got primary key for object " + _object);
				pk = _entity.pkGenerator().newPrimaryKeyForObject(_object, context);
				p.stop();
				_entity.setPrimaryKeyForObject(pk, _object);
			}
			
			if(pk == null) {
				DbLogger.rdbms.error("No primary key for object of entity '" + _entity.entityName() + "'");
				return;
			}
			
			Perf p = Perf.newPerf("Inserted " + _entity.entityName()  + "(" + pk + ")");
			if(_entity.inheritance() != null && _entity.parentEntity() != null && _entity.inheritance().isSingleTableInheritance()) {
				DbField discField = _entity.inheritance().discriminateField();
				String discValue = _entity.inheritance().discriminateValue();
				FieldAccess.Util.setValueForKey(_object, discValue, discField.key());
			}
						
			DbUpdater updater = this.getProcessorInContext(context);			
			DbSQLCommand insertCommand = _entity.createInsertCommand(_object, context);
			updater.session().executeUpdate(insertCommand);
			Map<String, Object> row = insertCommand.row();
			
			p.stop();
			
			context.didInsert(_object);

			DbEntityID entityId = updater.session().obtainEntityID(_entity, _entity.primaryKeyForObject(_object));
			if(entityId != null && updater != null)
				updater.session().recordSnapshot(entityId, row);
		}
		

	}

	public static boolean isInsertOperation(DbOperation operation) {
		return (operation instanceof Insert);
	}

	public static boolean isUpdateOperation(DbOperation operation) {
		return (operation instanceof Update);
	}

	public static boolean isDeleteOperation(DbOperation operation) {
		return (operation instanceof Delete);
	}

}
