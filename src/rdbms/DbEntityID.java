package rdbms;

public abstract class DbEntityID {
	DbEntity _entity;
	int _hashCode;
	
	protected DbEntityID(DbEntity entity) {
		_entity = entity;
	}
	@Override
	public int hashCode() {
		return _hashCode;
	}
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DbEntityID)
			return _hashCode == ((DbEntityID) obj)._hashCode;
		return false;
	}
	public DbEntity entity() {
		return _entity;
	}
	public String entityName() {
		return _entity.entityName();
	}
	
	public static class PK extends DbEntityID {
		private Object _primaryKey;
		
		public PK(DbEntity entity, Object primaryKey) {
			super(entity);
			this._primaryKey = primaryKey;
			this._hashCode = _hashCode();
		}
		public Object primaryKey() {
			return _primaryKey;
		}
		@Override
		public boolean equals(Object object) {
			if(object == this)
				return true;
			if(!(object instanceof PK))
				return false;
			PK gid = (PK)object;
			if(_primaryKey == gid._primaryKey && entityName().equals(gid.entityName()))
				return true;
			if(_hashCode != gid._hashCode)
				return false;
			if(!entityName().equals(gid.entityName()))
				return false;
			if(!_primaryKey.equals(gid._primaryKey))
				return false;
			return true;
		}
		
		private int _hashCode() {
			int code = entityName().hashCode();
	        if(_primaryKey != null)
	        	code ^= _primaryKey.hashCode();
	        return code == 0 ? 42 : code;
		}
		
		@Override
		public String toString() {
			return "{<" + Integer.toHexString(_hashCode) + "> "+ entityName() + ", " + _primaryKey + "}";
		}

	}
	
	public static class KeyValue extends DbEntityID {
		private String _key;
		private Object _value;
		
		public KeyValue(DbEntity entity, String key, Object value) {
			super(entity);
			_key = key;
			_value = value;
			_hashCode = _hashCode();
		}

		private int _hashCode() {
			int code = entityName().hashCode();
			if(_key != null)
				code ^= _key.hashCode();
	        if(_value != null)
	    		code ^= _value.hashCode();
	        return code == 0 ? 43 : code;
		}
		
		public String key() {
			return _key;
		}
		public Object value() {
			return _value;
		}

		@Override
		public boolean equals(Object object) {
			if(object == this)
				return true;
			if(!(object instanceof KeyValue))
				return false;
			KeyValue gid = (KeyValue)object;
			if(_value == gid._value && _key == gid._key && entityName().equals(gid.entityName()))
				return true;
			if(_hashCode != gid._hashCode)
				return false;
			if(!entityName().equals(gid.entityName()))
				return false;
			if(!_key.equals(gid._key))
				return false;
			if(!_value.equals(gid._value))
				return false;
			return true;
				
		}
		
		@Override
		public String toString() {
			return "{<" + Integer.toHexString(_hashCode) + "> "+ entityName() + ", " + _key + ", " + _value + "}";
		}

	}
}
