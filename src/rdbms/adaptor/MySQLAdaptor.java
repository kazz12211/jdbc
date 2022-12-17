package rdbms.adaptor;

import java.sql.Connection;

import rdbms.DbAdaptor;
import rdbms.DbModel;

public class MySQLAdaptor extends DbAdaptor {

	public MySQLAdaptor(DbModel model) {
		super(model);
	}

	@Override
	public boolean supportsSequence() {
		return false;
	}

	@Override
	protected int defaultIsolationLevel() {
		return Connection.TRANSACTION_REPEATABLE_READ;
	}

}
