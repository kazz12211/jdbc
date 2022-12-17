package rdbms;

import java.util.List;
import java.util.Map;

import core.util.MapUtils;

public class DbRowID {
	Map<String, Object> _ids = MapUtils.map();
	
	public DbRowID(String key, Object value) {
		_ids.put(key, value);
	}
	
	public DbRowID(String keys[], Object values[]) {
		int size = keys.length > values.length ? values.length : keys.length;
		for(int i = 0; i < size; i++) {
			_ids.put(keys[i], values[i]);
		}
	}
	public DbRowID(List<String> keys, List<Object> values) {
		int size = keys.size() > values.size() ? values.size() : keys.size();
		for(int i = 0; i < size; i++) {
			_ids.put(keys.get(i), values.get(i));
		}
	}
	
	public Map<String, Object> ids() {
		return _ids;
	}
	
	@Override
	public boolean equals(Object other) {
		return MapUtils.mapEquals(_ids, ((DbRowID) other)._ids);
	}
}
