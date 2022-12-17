package rdbms.object;

import core.util.FieldAccess;
import rdbms.DbContext;

public interface DbFaulting {

	public Object getStoredValueForRelationshipWithKey(String key);
	
	public static class DefaultImplementation {
		
		public Object getStoredValueForRelationshipWithKey(Object object, String key) {
			Object value = FieldAccess.DefaultImplementation.getValueForKey(object, key);
			if(value == null) {
				value = DbContext.get().storedValueForToOneRelationship(object, key);
				FieldAccess.DefaultImplementation.setValueForKey(object, value, key);
			}
			return value;
		}
	}
}
