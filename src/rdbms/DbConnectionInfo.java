package rdbms;

import java.util.Properties;

public class DbConnectionInfo {
	String _driverClass;
	String _url;
	String _username;
	String _password;
	Properties _properties;
	
	public String driverClass() {
		return _driverClass;
	}
	public void setDriverClass(String driverClass) {
		_driverClass = driverClass;
	}
	public String url() {
		return _url;
	}
	public void setUrl(String url) {
		_url = url;
	}
	public String username() {
		return _username;
	}
	public void setUsername(String username) {
		_username = username;
	}
	public String password() {
		return _password;
	}
	public void setPassword(String password) {
		_password = password;
	}
	public Properties properties() {
		return _properties;
	}
	public void setProperties(Properties properties) {
		_properties = properties;
	}
	
	@Override
	public String toString() {
		return "{driverClass=" + _driverClass + "; username=" + _username + "; password=******" + "; url=" + _url + "}"; 
	}

}
