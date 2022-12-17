package rdbms.command;

import java.util.List;
import java.util.Map;

import rdbms.DbEntity;
import rdbms.DbField;
import core.util.ListUtils;
import core.util.MapUtils;

public class SQLGenerationContext {

	Map<DbEntity, String> _aliases = MapUtils.map();
	
	public String aliasForEntity(DbEntity entity) {
		if(_aliases.containsKey(entity)) {
			return _aliases.get(entity);
		} else {
			String entityName = entity.entityName();
			String alias = entityName + "_" + _aliases.size();
			_aliases.put(entity, alias);
			return alias;
		}
	}
	
	public List<String> columnNamesForEntity(DbEntity entity) {
		String alias = aliasForEntity(entity);
		List<String> columnNames = ListUtils.list();
		for(DbField field : entity.fields()) {
			columnNames.add(alias + "." + field.columnName());
		}
		return columnNames;
	}

	public String columnNameForField(DbField field, DbEntity entity) {
		String alias = aliasForEntity(entity);
		return alias + "." + field.columnName();
	}
	
}
