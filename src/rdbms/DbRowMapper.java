package rdbms;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import rdbms.util.DbLogger;
import core.util.Accessor;
import core.util.CharacterSet;
import core.util.ClassUtils;
import core.util.FieldAccess;
import core.util.ListUtils;
import core.util.MapUtils;

public abstract class DbRowMapper {

	private static byte[] _NoBytes = new byte[0];
	Class<?> _objectClass;
	List<String> _columnNames;
	List<String> _keys;
	Map<String, DateFormat> _dateFormats = MapUtils.map();
	Map<String, ValueConverter> _converters = MapUtils.map();
	
	protected DbRowMapper(Class<?> objectClass) {
		_objectClass = objectClass;
	}
	
	public void setColumnNames(List<String> columnNames) {
		_columnNames = columnNames;
	}
	public void setKeys(List<String> keys) {
		_keys = keys;
	}
	
	protected Class<?> valueClassForKey(String key) {
		Accessor accessor = Accessor.newGetAccessor(_objectClass, key);
		Class<?> valueClass = accessor.getReturnType();
		return valueClass;
	}
	public Object createObjectFromRow(Map<String, Object> row) {
		
		int size = _columnNames.size() > _keys.size() ? _keys.size() : _columnNames.size();
		Object obj = ClassUtils.newInstance(_objectClass);
		if(obj != null) {
			for(int i = 0; i < size; i++) {
				String colName = _columnNames.get(i);
				String key = _keys.get(i);
				Object o = row.get(colName);
				Class<?> valueClass = valueClassForKey(key);
				ValueConverter converter = _converters.get(key);
				Object value = null;
				if(converter != null)
					value = converter.convertObject(o, valueClass, key);
				else
					value = coerceValue(o, valueClass, key);
				FieldAccess.DefaultImplementation.setValueForKey(obj, value, key);
			}
		}
		return obj;
	}

	private Number coerceToNumber(Object value, Class<?> valueClass) {
		if(value == null)
			return null;
	
		String str = value.toString();
		
		if(str == null || str.length() == 0)
			return null;
		
		Number result = null;
		CharacterSet cset = new CharacterSet("0123456789.");
		str = cset.stringContainingCharacters(str);
		try {
			if(valueClass.equals(java.lang.Integer.class))
				result = new java.lang.Integer(str);
			if(valueClass.equals(java.lang.Short.class))
				result = new java.lang.Short(str);
			if(valueClass.equals(java.lang.Long.class))
				result = new java.lang.Long(str);
			if(valueClass.equals(java.lang.Float.class))
				result = new java.lang.Float(str);
			if(valueClass.equals(java.lang.Double.class))
				result = new java.lang.Double(str);
			if(valueClass.equals(java.math.BigDecimal.class))
				result = new java.math.BigDecimal(str);
			if(valueClass.equals(java.math.BigInteger.class))
				result = new java.math.BigInteger(str);
		} catch (Exception e) {
			DbLogger.rdbms.warn("coerceToNumber(): The value " + value + " seems not to be a number");
		}
		return result;
	}
	
	public Object coerceValue(Object value, Class<?> valueClass, String key) {
		if(value == null)
			return null;
		if(valueClass == null)
			return value.toString();
		
		if(value instanceof Collection) {
			List<Object> values = ListUtils.list();
			for(Object obj : ((Collection)value)) {
				values.add(this.coerceValue(obj, valueClass, key));
			}
			return values;
		}
		
		if(valueClass.isAssignableFrom(value.getClass()))
			return value;
		
		if(valueClass.equals(java.lang.String.class))
			return value.toString();
		
		if(valueClass.equals(java.lang.Integer.class)) {
			if(!(value instanceof java.lang.Number))
				return coerceToNumber(value, valueClass);
			else
				return new java.lang.Integer(((java.lang.Number) value).intValue());
		}
		if(valueClass.equals(java.lang.Short.class)) {
			if(!(value instanceof java.lang.Number))
				return coerceToNumber(value, valueClass);
			else
				return new java.lang.Short(((java.lang.Number) value).shortValue());
		}
		if(valueClass.equals(java.lang.Long.class)) {
			if(!(value instanceof java.lang.Number))
				return coerceToNumber(value, valueClass);
			else
				return new java.lang.Long(((java.lang.Number) value).longValue());
		}
		if(valueClass.equals(java.lang.Float.class)) {
			if(!(value instanceof java.lang.Number))
				return coerceToNumber(value, valueClass);
			else
				return new java.lang.Float(((java.lang.Number) value).floatValue());
		}
		if(valueClass.equals(java.lang.Double.class)) {
			if(!(value instanceof java.lang.Number))
				return coerceToNumber(value, valueClass);
			else
				return new java.lang.Double(((java.lang.Number) value).doubleValue());
				
		}
		if(valueClass.equals(java.math.BigInteger.class)) {
			if(!(value instanceof java.lang.Number))
				return coerceToNumber(value, valueClass);
			else
				return new java.math.BigInteger(value.toString());
		}
		if(valueClass.equals(java.math.BigDecimal.class)) {
			if(!(value instanceof java.lang.Number))
				return coerceToNumber(value, valueClass);
			else
				return new java.math.BigDecimal(value.toString());
		}
		if(valueClass.equals(java.util.Date.class)) {
			if(value == null || value.toString().length() == 0)
				return null;
			DateFormat format = _dateFormats.get(key);
			if(format == null)
				format = new SimpleDateFormat("yyyy/MM/dd");
			try {
				return format.parseObject(value.toString());
			} catch (ParseException e) {
				DbLogger.rdbms.warn("UniField coerceValue() could not parse date string with format '" + format + "'", e);
				return null;
			}
		}
		if(valueClass.equals(java.lang.Boolean.class)) {
			if(!(value instanceof java.lang.Boolean))
				return new java.lang.Boolean(((Boolean) value).booleanValue());
			else {
				String s = value.toString();
				if("true".equalsIgnoreCase(s) || "yes".equalsIgnoreCase(s) || "1".equals(s))
					return new java.lang.Boolean(true);
				else
					return new java.lang.Boolean(false);
			}
		}
		if(valueClass.equals(java.lang.Byte.class)) {
		}
		if(valueClass.equals(java.lang.Character.class)) {
		}
		if(valueClass.equals(_NoBytes.getClass())) {
		}
		return value;

	}
	
	public void registerConverter(ValueConverter converter, String key) {
		_converters.put(key, converter);
	}
	public void registerDateFormat(DateFormat format, String key) {
		_dateFormats.put(key, format);
	}
	
	public interface ValueConverter {
		public Object convertObject(Object value, Class<?> valueClass, String propertyKey);
	}
}
