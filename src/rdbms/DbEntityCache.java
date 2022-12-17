package rdbms;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import rdbms.util.DbLogger;
import core.util.MapUtils;

public class DbEntityCache {

	Map<DbEntityID, CachedRecord> _cache = MapUtils.map();
	private static final long SHORT_LIFE = 30000L;
	private static final long LONG_LIFE = 180000L;
	private static final long DistantFuture = 1000L * 60L * 60L * 24L;
	private Stat _stat = new Stat();
	
	protected class CachedRecord {
		Object _object;
		long _timestamp;
		
		@Override
		public String toString() {
			return "{object=" + _object.toString() + "; timestamp=" + new Date(_timestamp) + "}";
		}

	}

	public Object add(DbEntityID entityId, Object object) {
		_stat.add();
		if(entityId.entity().cacheStrategy() == DbEntity.CacheStrategy.None)
			return object;
		
		Object obj = null;
		
		synchronized(_cache) {
			if(this.contains(entityId)) {
				obj = replace(entityId, object);
				DbLogger.rdbms_cache.debug("[CACHE] " + object.toString() + " with entityId(" + entityId + ") replaced");
			} else {
				CachedRecord record = new CachedRecord();
				record._object = object;
				record._timestamp = this._cacheExpiration(entityId);
				_cache.put(entityId, record);
				DbLogger.rdbms_cache.debug("[CACHE] " + object.toString() + " with entityId(" + entityId + ") cached");
				obj =  object;
			}
		}
		
		return obj;
	}
	
	private Object replace(DbEntityID entityId, Object object) {
		CachedRecord record = _cache.get(entityId);
		record._object = object;
		record._timestamp = this._cacheExpiration(entityId);
		_cache.put(entityId, record);
		return object;
	}
		
	public Object get(DbEntityID entityId) {
		if(entityId.entity().cacheStrategy() == DbEntity.CacheStrategy.None)
			return null;
		
		Object object = null;
		
		synchronized(_cache) {
			CachedRecord record = _cache.get(entityId);
			if(record != null && record._timestamp >= System.currentTimeMillis()) {
				DbLogger.rdbms_cache.debug("[CACHE] Returns cached object " + record._object +  " with entityId(" + entityId + ")");
				object = record._object;
			} else if(record != null) {
				_cache.remove(entityId);
			}
		}
		
		return object;
	}
			
	public boolean contains(DbEntityID entityId) {
		boolean found = false;
		synchronized(_cache) {
			found = _cache.containsKey(entityId);
		}
		return found;
	}
	
	public void remove(DbEntityID entityId) {
		synchronized(_cache) {
			if(contains(entityId)) {
				_cache.remove(entityId);
				DbLogger.rdbms_cache.debug("[CACHE] Removed from cache " + entityId);
			}
		}
	}
	
	public void clear() {
		synchronized(_cache) {
			DbLogger.rdbms_cache.debug("[CACHE] Cleared cache");
			_cache.clear();
		}
	}
	
	public void clearExpiredRecords() {
		synchronized(_cache) {
			long millis = System.currentTimeMillis();
			Set<Map.Entry<DbEntityID, CachedRecord>> entries = _cache.entrySet();
			for(Map.Entry<DbEntityID, CachedRecord> entry : entries) {
				if(entry.getValue()._timestamp < millis)
					_cache.remove(entry.getKey());
			}
		}
	}
	
	private long _cacheExpiration(DbEntityID entityId) {
		DbEntity.CacheStrategy strategy = entityId.entity().cacheStrategy();
		if(DbEntity.CacheStrategy.None == strategy)
			return System.currentTimeMillis();
		if(DbEntity.CacheStrategy.Normal == strategy)
			return System.currentTimeMillis() + SHORT_LIFE;
		if(DbEntity.CacheStrategy.Statistical == strategy)
			return System.currentTimeMillis() + _stat.goodValue();
		if(DbEntity.CacheStrategy.DistantFuture == strategy)
			return System.currentTimeMillis() + DistantFuture;
		return System.currentTimeMillis() + SHORT_LIFE;
	}
	

	class Stat {
		long _lastTime;
		long _previousTime;
		long _minInterval = SHORT_LIFE;
		long _maxInterval = LONG_LIFE;
		
		public void add() {
			if(_lastTime == 0)
				_previousTime = System.currentTimeMillis() - LONG_LIFE;
			else
				_previousTime = _lastTime;
			_lastTime = System.currentTimeMillis();
			if(_minInterval > (_lastTime - _previousTime))
				_minInterval = _lastTime - _previousTime;
			if(_maxInterval < (_lastTime - _previousTime))
				_maxInterval = _lastTime - _previousTime;
		}
		
		public long goodValue() {
			return (_minInterval + _maxInterval) / 2 + SHORT_LIFE;
		}
	}
}
