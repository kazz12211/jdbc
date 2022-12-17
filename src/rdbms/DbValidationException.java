package rdbms;

import java.util.Map;

public class DbValidationException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	Map<String, Object> _userInfo;
	DbOperation _operation;

	public DbValidationException(String message, Throwable throwable, Map<String, Object> userInfo) {
		super(message, throwable);
		_userInfo = userInfo;
	}
	public DbValidationException(String message, Map<String, Object> userInfo) {
		super(message);
		_userInfo = userInfo;
	}
	public DbValidationException(Throwable throwable, Map<String, Object> userInfo) {
		super(throwable);
		_userInfo = userInfo;
	}
	public Map<String, Object> userInfo() {
		return _userInfo;
	}
	public DbOperation operation() {
		return _operation;
	}
	public void setOperation(DbOperation operation) {
		_operation = operation;
	}
}
