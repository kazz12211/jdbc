package rdbms;

public abstract class DbAccess {
	DbSession _session;
	
	protected DbAccess(DbSession session) {
		this._session = session;
	}
	protected boolean isConnected() {
		return _session.adaptor().isConnected();
	}
	public DbSession session() {
		return _session;
	}
}
