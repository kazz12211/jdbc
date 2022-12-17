package rdbms;

import java.util.Collection;

import rdbms.util.DbLogger;
import core.util.FieldAccess;

public class DbRelationship {

	DbEntity _entity;
	String _key;
	boolean _isToMany;
	boolean _shouldPrefetch = false;
	String _destinationEntityName;
	String _sourceKey;
	String _destinationKey;
	boolean _ownsDestination;
	boolean _cacheDestination = true;

	public DbRelationship(DbEntity sourceEntity, String destinationEntity, String key, String sourceKey, String destinationKey, boolean isToMany, boolean ownsDestination) {
		_entity = sourceEntity;
		_key = key;
		_isToMany = isToMany;
		_destinationEntityName = destinationEntity;
		_sourceKey = sourceKey;
		_destinationKey = destinationKey;
		_ownsDestination = ownsDestination;
	}
	
	public String key() {
		return _key;
	}
	public void setKey(String key) {
		this._key = key;
	}
	public boolean isToMany() {
		return _isToMany;
	}
	public void setToMany(boolean isToMany) {
		this._isToMany = isToMany;
	}

	public DbEntity entity() {
		return _entity;
	}
	public void setEntity(DbEntity entity) {
		this._entity = entity;
	}
	public boolean shouldPrefetch() {
		return _shouldPrefetch;
	}
	public void setShoudPrefetch(boolean flag) {
		this._shouldPrefetch = flag;
	}

	public DbEntity sourceEntity() {
		return _entity;
	}
	public void setSourceEntity(DbEntity source) {
		_entity = source;
	}
	public String destinationEntityName() {
		return _destinationEntityName;
	}
	public void setDestinationEntityName(String destination) {
		this._destinationEntityName = destination;
	}
	
	public String sourceKey() {
		return _sourceKey;
	}
	public String destinationKey() {
		return _destinationKey;
	}
	public boolean ownsDestination() {
		return _ownsDestination;
	}
	public DbEntity destinationEntity() {
		DbModel model = _entity.model();
		return model != null ? model.entityNamed(_destinationEntityName) : null;
	}
	
	public String toString() {
		return "join {key=" + _key + "; targetEntity=" + _destinationEntityName + "}";
	}

	public boolean cacheDestination() {
		return _cacheDestination;
	}

	public static void addObjectToBothSidesOfRelationshipWithKey(
			Object object, Object value, String key) {
		DbEntity sourceEntity = DbContext.get().entityForObject(object);
		DbEntity destEntity = DbContext.get().entityForObject(value);
		if(sourceEntity == null || destEntity == null)
			return;
		DbRelationship rel = sourceEntity.relationshipNamed(key);
		if(rel == null)
			return;
		DbRelationship reverseJoin = null;
		for(DbRelationship join : destEntity.relationships()) {
			if(join.destinationEntity() == sourceEntity) {
				reverseJoin = join; break;
			}
		}
		if(rel.isToMany()) {
			Collection col = (Collection) FieldAccess.Util.getValueForKey(object, rel.key());
			col.add(value);
			String sourceKey = rel.sourceKey();
			String destKey = rel.destinationKey();
			Object sourceValue = FieldAccess.Util.getValueForKey(object, sourceKey);
			FieldAccess.Util.setValueForKey(value, sourceValue, destKey);
			DbContext.get().updateObject(object);
			if(reverseJoin != null) {
				FieldAccess.Util.setValueForKey(value, object, reverseJoin.key());
			}
		} else {
			FieldAccess.Util.setValueForKey(object, value, rel.key());
			String sourceKey = rel.sourceKey();
			String destKey = rel.destinationKey();
			Object sourceValue = FieldAccess.Util.getValueForKey(object, sourceKey);
			FieldAccess.Util.setValueForKey(value, sourceValue, destKey);
			if(reverseJoin != null) {
				FieldAccess.Util.setValueForKey(value, object, reverseJoin.key());
			}
		}
		DbContext.get().updateObject(value);
	}

	public static void removeObjectFromBothSidesOfRelationshipWithKey(
			Object object, Object value, String key) {
		DbEntity sourceEntity = DbContext.get().entityForObject(object);
		DbEntity destEntity = DbContext.get().entityForObject(value);
		if(sourceEntity == null || destEntity == null)
			return;
		DbRelationship rel = sourceEntity.relationshipNamed(key);
		if(rel == null)
			return;
		if(rel.isToMany()) {
			Collection col = (Collection) FieldAccess.Util.getValueForKey(object, rel.key());
			DbLogger.rdbms_test.info("**** Before delete " + col);
			DbLogger.rdbms_test.info("**** Removing " + value);
			DbEntityID eid = DbContext.get().entityIDForObject(value);
			for(Object obj : col) {
				DbEntityID eid2 = DbContext.get().entityIDForObject(obj);
				if(eid.equals(eid2)) {
					col.remove(obj);
					break;
				}
			}
			DbLogger.rdbms_test.info("**** After delete " + col);
			String destKey = rel.destinationKey();
			FieldAccess.Util.setValueForKey(value, null, destKey);
		} else {
			FieldAccess.Util.setValueForKey(object, null, rel.key());
			String sourceKey = rel.sourceKey();
			String destKey = rel.destinationKey();
			FieldAccess.Util.setValueForKey(value, null, destKey);
			FieldAccess.Util.setValueForKey(object, null, sourceKey);
			DbContext.get().updateObject(object);
		}
		
		if(rel.ownsDestination()) {
			DbContext.get().deleteObject(value);
		}
	}

}
