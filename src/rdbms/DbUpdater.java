package rdbms;

import java.util.ArrayList;
import java.util.List;

import core.util.ListUtils;
import core.util.Perf;
import core.util.Selector;

public class DbUpdater extends DbAccess {

	private boolean _isActive = false;
	private List<DbOperation> _inserts = ListUtils.list();
	private List<DbOperation> _updates = ListUtils.list();
	private List<DbOperation> _deletes = ListUtils.list();
	private boolean _hasChanges = false;
	private boolean _autoCommit = false;
	private static Selector validateForSaveSelector = new Selector("validateForSave", new Class[]{DbContext.class});
	private static Selector validateForDeleteSelector = new Selector("validateForDelete", new Class[]{DbContext.class});
	
	public DbUpdater(DbSession session) {
		super(session);
	}
	
	public boolean isAutoCommit() {
		return _autoCommit;
	}
	public void setAutoCommit(boolean flag) {
		_autoCommit = flag;
	}
	
	public void begin() throws Exception {
		if(!_isActive) {
			if(!_autoCommit)
				session().beginTransaction();
			_isActive = true;
		}
	}

	public void commit() throws Exception {
		if(_isActive) {
			if(!_autoCommit)
				session().commitTransaction();
			_isActive = false;
		}
		clear();
	}

	public void rollback() {
		if(_isActive) {
			if(!_autoCommit)
				session().rollbackTransaction();
			_isActive = false;
		}
		clear();
	}

	protected void clear() {
		_inserts.clear();
		_updates.clear();
		_deletes.clear();
		_hasChanges  = false;
	}
	
	public boolean hasChanges() {
		return _hasChanges;
	}
	
	private DbOperation findOperation(DbOperation opr, List<DbOperation> operations) {
		for(DbOperation operation : operations) {
			if(operation.object().equals(opr.object()) && operation.entity().equals(opr.entity()))
				return operation;
		}
		return null;
	}
	
	public void insert(Object object, DbContext DbContext) {
		DbEntity entity = DbContext.entityForObject(object);
		DbOperation opr = new DbOperation.Insert(object, entity);
		DbOperation found = findOperation(opr, _inserts);
		if(found != null) {
			found._millis = System.currentTimeMillis();
			found._object = object;
		} else {
			_inserts.add(new DbOperation.Insert(object, entity));
		}
		_hasChanges = true;
	}

	public void delete(Object object, DbContext DbContext) {
		DbEntity entity = DbContext.entityForObject(object);
		DbOperation opr = new DbOperation.Delete(object, entity);
		DbOperation found = findOperation(opr, _deletes);
		if(found != null) {
			found._millis = System.currentTimeMillis();
			found._object = object;
		} else {
			_deletes.add(new DbOperation.Delete(object, entity));
		}
		_hasChanges = true;
	}

	public void update(Object object, DbContext DbContext) {
		DbEntity entity = DbContext.entityForObject(object);
		DbOperation opr = new DbOperation.Update(object, entity);
		DbOperation found = findOperation(opr, _updates);
		if(found != null) {
			found._millis = System.currentTimeMillis();
			found._object = object;
		} else {
			_updates.add(new DbOperation.Update(object, entity));
		}
		_hasChanges = true;
	}

	private List<DbOperation> orderedOperations() {
		List<DbOperation> operations = new ArrayList<DbOperation>();
		operations.addAll(DbOperation.sortedOperations(_inserts));
		operations.addAll(DbOperation.sortedOperations(_updates));
		operations.addAll(DbOperation.sortedOperations(_deletes));
		return operations;
	}
	
	public void executeDatabaseOperations(DbContext DbContext) {
		List<DbOperation> orderedOperations = this.orderedOperations();
		if(DbContext.shouldFilterOperations())
			orderedOperations = DbContext.filterOperations(orderedOperations);
		if(DbContext.shouldOrderOperations())
			orderedOperations = DbContext.orderOperations(orderedOperations);
		int numInsert = 0;
		int numUpdate = 0;
		int numDelete = 0;
		int numOps = orderedOperations.size();
		Perf p = Perf.newPerf("Finished database operations");
		DbOperation lastOperation = null;
		try {
			for(DbOperation operation : orderedOperations) {
				lastOperation = operation;
				if(DbOperation.isInsertOperation(operation)) {
					if(Selector.objectRespondsTo(operation.object(), validateForSaveSelector)) {
						validateForSaveSelector.invoke(operation.object(), new Object[]{DbContext});
					}
					operation.executeInContext(DbContext);
					numInsert++;
				} 
				if(DbOperation.isUpdateOperation(operation)) {
					if(Selector.objectRespondsTo(operation.object(), validateForSaveSelector)) {
						validateForSaveSelector.invoke(operation.object(), new Object[]{DbContext});
					}
					operation.executeInContext(DbContext);
					numUpdate++;
				} 
				if(DbOperation.isDeleteOperation(operation)) {
					if(Selector.objectRespondsTo(operation.object(), validateForDeleteSelector)) {
						validateForDeleteSelector.invoke(operation.object(), new Object[]{DbContext});
					}
					operation.executeInContext(DbContext);
					numDelete++;
				}
			}
		} catch (DbValidationException invalid) {
			invalid.setOperation(lastOperation);
			throw invalid;
		} catch (Exception e) {
			throw new IllegalStateException(lastOperation.toString(), e);
		} finally {
			p.stop("Total " + numOps + " operations (inserts=" + numInsert + ", updates=" + numUpdate + ", deletes=" + numDelete);
		}
	}

	public void remove(Object object, DbContext dbContext) {
		DbEntity entity = dbContext.entityForObject(object);
		for(DbOperation opr : _inserts) {
			if(opr.object().equals(object) && opr.entity().equals(entity)) {
				_inserts.remove(opr);
				break;
			}
		}
	}

}
