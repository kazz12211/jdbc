package rdbms.adaptor;

import java.sql.Connection;

import rdbms.DbAdaptor;
import rdbms.DbModel;

public class PGSQLAdaptor extends DbAdaptor {

	public PGSQLAdaptor(DbModel model) {
		super(model);
	}

	@Override
	public boolean supportsSequence() {
		return true;
	}

	@Override
	protected int defaultIsolationLevel() {
		return Connection.TRANSACTION_READ_COMMITTED;
	}

}
