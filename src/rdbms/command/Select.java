package rdbms.command;

import java.util.List;

import core.util.ListUtils;
import rdbms.DbEntity;
import rdbms.DbQuerySpecification;
import rdbms.DbSQLCommand;
import rdbms.DbSortOrdering;

public class Select extends DbCommand {
	
	public Select(DbQuerySpecification spec) {
		super(spec);
	}

	public DbSQLCommand sqlCommand() throws Exception {
		DbEntity entity = querySpecification().entity();
		StringBuffer sql = new StringBuffer();
		List<String> columnNames = generationContext().columnNamesForEntity(entity);
		String alias = generationContext().aliasForEntity(entity);
		sql.append("SELECT " + ListUtils.listToString(columnNames, ", ") + " FROM " + entity.tableName() + " " + alias);
		if(querySpecification().predicate() != null) {
			sql.append(" WHERE ");
			//sql.append(querySpecification().predicate().generateString(entity));
			sql.append(querySpecification().predicate().generateStringInContext(entity, generationContext()));
		}
		if(!ListUtils.nullOrEmpty(querySpecification().sortOrderings())) {
			sql.append(" ORDER BY ");
			List<String> soStrings = ListUtils.list();
			for(DbSortOrdering so : querySpecification().sortOrderings()) {
				soStrings.add(so.generateStringInContext(entity, generationContext()));
			}
			sql.append(ListUtils.listToString(soStrings, ", "));
		}
		DbSQLCommand command = new DbSQLCommand(entity, sql.toString());
		return command;
	}


}
