package rdbms.command;

import java.util.List;

import core.util.ListUtils;
import rdbms.DbEntity;
import rdbms.DbQuerySpecification;
import rdbms.DbSQLCommand;

public class AggregateFunctions extends DbCommand {
	String key;

	public AggregateFunctions(String key, DbQuerySpecification spec) {
		super(spec);
		this.key = key;
	}
	
	public String key() {
		return key;
	}
	
	public DbSQLCommand sqlCommand() throws Exception {
		DbEntity entity = querySpecification().entity();
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT ");
		String columnName = entity.fieldNamed(key).columnName();

		List<String> aggrStrings = ListUtils.list();
		aggrStrings.add("SUM(" + columnName + ")");
		aggrStrings.add("COUNT(" + columnName + ")");
		aggrStrings.add("MIN(" + columnName + ")");
		aggrStrings.add("MAX(" + columnName + ")");
		aggrStrings.add("AVG(" + columnName + ")");
		sql.append(ListUtils.listToString(aggrStrings, ", "));
		sql.append(" FROM " + entity.tableName());
		if(querySpecification().predicate() != null) {
			sql.append(" WHERE ");
			sql.append(querySpecification().predicate().generateString(entity));
		}
		DbSQLCommand command = new DbSQLCommand(entity, sql.toString());
		return command;
	}

}
