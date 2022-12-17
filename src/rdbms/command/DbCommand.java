package rdbms.command;

import rdbms.DbQuerySpecification;

public abstract class DbCommand {
	DbQuerySpecification _spec;
	SQLGenerationContext _ctx = new SQLGenerationContext();
	
	protected DbCommand(DbQuerySpecification spec) {
		_spec = spec;
	}
	public DbQuerySpecification querySpecification() {
		return _spec;
	}
	public SQLGenerationContext generationContext() {
		return _ctx;
	}
}
