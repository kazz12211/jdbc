package rdbms;

import java.util.List;
import java.util.Map;

import core.util.ListUtils;


public class DbQuerySpecification {
	DbEntity _entity;
	Class<?> _entityClass;
	DbPredicate _predicate;
	List<DbSortOrdering> _sortOrderings;
	Map<String, Integer> _fetchHint;
	
	public static final String FetchLimitSize = "fetchLimitSize";
	public static final String FetchLimitStart = "fetchLimitStart";
	boolean _forceRefetch = false;
		
	public DbQuerySpecification(DbEntity entity, DbPredicate predicate) {
		this._entity = entity;
		this._entityClass = entity.entityClass();
		this._predicate = predicate;
	}
	
	public Class<?> entityClass() {
		return _entityClass;
	}

	public DbEntity entity() {
		if(_entity == null && _entityClass != null) {
			return null;
		}
		return _entity;
	}
	
	public DbPredicate predicate() {
		return _predicate;
	}
	
	public void setSortOrderings(List<DbSortOrdering> sortOrderings) {
		this._sortOrderings = sortOrderings;
	}
	
	public List<DbSortOrdering> sortOrderings() {
		if(_sortOrderings == null) {
			_sortOrderings = ListUtils.list();
		}
		return _sortOrderings;
	}
	
	public void setFetchHint(Map<String, Integer> hint) {
		_fetchHint = hint;
	}
	public Map<String, Integer> fetchHint() {
		return _fetchHint;
	}
	public boolean forceRefetch() {
		return _forceRefetch;
	}
	public void setForceRefetch(boolean flag) {
		_forceRefetch = flag;
	}

}
