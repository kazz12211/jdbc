package rdbms;

import java.util.List;
import java.util.Map;

import core.util.ListUtils;
import core.util.MapUtils;

public class DbModel {
	String _name;
	Map<Class<?>, DbEntity> _entityMap = MapUtils.map();
	List<DbEntity> _entities = ListUtils.list();
	DbConnectionInfo _connectionInfo;
	
	public static DbModel modelNamed(String modelName) {
		return DbModelGroup.modelNamed(modelName);
	}
	public List<DbEntity> entities() {
		return _entities;
	}
	public void addEntity(Class<?> entityClass, DbEntity entity) {
		_entityMap.put(entityClass, entity);
		_entities.add(entity);
	}
	public void addEntity(DbEntity entity) {
		addEntity(entity.entityClass(), entity);
	}
	public void removeEntity(DbEntity entity) {
		Class<?> entityClass = entity.entityClass();
		_entityMap.remove(entityClass);
		_entities.remove(entity);
	}
	public DbEntity entityForClass(Class<?> entityClass) {
		return _entityMap.get(entityClass);
	}
	public DbEntity entityNamed(String entityName) {
		for(DbEntity entity : _entities) {
			if(entity.entityName().equals(entityName))
				return entity;
		}
		return null;
	}
	public DbConnectionInfo connectionInfo() {
		return _connectionInfo;
	}
	public String name() {
		return _name;
	}
	public void setName(String name) {
		_name = name;
	}
	
	@Override
	public String toString() {
		return "{name="+_name+"; entities=" + _entities.toString() + "; connectionInfo=" + _connectionInfo.toString() + "}";
	}
}
