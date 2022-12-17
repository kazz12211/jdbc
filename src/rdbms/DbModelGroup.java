package rdbms;

import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.w3c.dom.Element;

import rdbms.util.DbLogger;
import ariba.ui.widgets.XMLUtil;
import core.util.CharacterSet;
import core.util.ClassUtils;
import core.util.ListUtils;
import core.util.MapUtils;
import core.util.StringUtils;
import core.util.XMLConfig;

public class DbModelGroup {
	private static DbModelGroup _defaultModelGroup = null;
	Map<String, DbModel> _models = MapUtils.map();

	public static DbModelGroup defaultGroup() {
		if(_defaultModelGroup == null) {
			_defaultModelGroup = new DbModelGroup();
			_defaultModelGroup.initWithFile("DbModel.xml");
		}
		return _defaultModelGroup;
	}
	
	private DbModel _modelNamed(String modelName) {
		return _models.get(modelName);
	}
	
	private Map<String, DbModel> _models() {
		return _models;
	}
	
	private void _addModel(DbModel model) {
		_models.put(model.name(), model);
	}
	
	public static DbModel modelNamed(String modelName) {
		return defaultGroup()._modelNamed(modelName);
	}
	
	public static Map<String, DbModel> models() {
		return defaultGroup()._models();
	}
	
	public static void addModel(DbModel model) {
		defaultGroup()._addModel(model);
	}
	
	private void initWithFile(String fileName) {
		Parser parser = new Parser();
		try {
			parser.parse(fileName);
		} catch (Exception e) {
			DbLogger.rdbms.error("DbModelGroup : could not parse model file '" + fileName + "'", e);
		}
	}
	
	protected Iterator<DbModel> modelIterator() {
		return _models.values().iterator();
	}
	
	public DbEntity entityNamed(String entityName) {
		DbEntity entity = null;
		for(Iterator<DbModel> iter = modelIterator(); iter.hasNext(); ) {
			DbModel model = iter.next();
			entity = model.entityNamed(entityName);
			if(entity != null)
				return entity;
		}
		return null;
	}

	public DbEntity entityForClass(Class<?> aClass) {
		DbEntity entity = null;
		for(Iterator<DbModel> iter = modelIterator(); iter.hasNext(); ) {
			DbModel model = iter.next();
			entity = model.entityForClass(aClass);
			if(entity != null)
				return entity;
		}
		return null;
	}
	
	@Override
	public String toString() {
		return _models.toString();
	}
	
	static class Parser extends XMLConfig {
		
		Map<String, Map<String, String>> _inheritanceDefs;
		
		public void parse(String filename) throws Exception {
			String path = super.getResourcePath(filename);
			DbLogger.rdbms.debug("DbModelGroup : loading models from '" + path + "'");
			URL url = new URL(path);
			Element docElement = XMLUtil.document(url, false, false, null).getDocumentElement();
			if("models".equals(docElement.getNodeName())) {
				Element[] modelElements = this.elementsNamed(docElement, "model");
				if(modelElements == null || modelElements.length == 0) {
					DbLogger.rdbms.warn("DbModelGroup : no model definitions in '" + path + "'");
				} else {
					_inheritanceDefs = MapUtils.map();
					
					for(Element element : modelElements) {
						DbModel model = new DbModel();
						initWithElement(model, element);
						if(model.name() != null) {
							DbModelGroup.addModel(model);
						}
					}
					
					makeInheritance();
				}
				
			}
		}

		private void initWithElement(DbModel model, Element element) {
			String modelName = element.getAttribute("name");
			model.setName(modelName);
			Element[] entityElements = this.elementsNamed(element, "entity");
			if(entityElements == null || entityElements.length == 0) {
				DbLogger.rdbms.warn("DbModelGroup : no entity definitions in model '" + modelName + "'");
			} else {
				for(Element elem : entityElements) {
					parseEntity(model, elem);
				}
			}
			Element connElem = this.elementNamed(element, "connection");
			if(connElem != null) {
				paserConnectionInfo(model, connElem);
			} else {
				DbLogger.rdbms.warn("DbModelGroup : no connection info definitions in model '" + modelName + "'");
			}
		}

		private void parseEntity(DbModel model, Element elem) {
			String entityName = elem.getAttribute("name");
			String tableName = elem.getAttribute("tableName");
			String entityClass = elem.getAttribute("class");
			String lockingStrategy = elem.getAttribute("lockingStrategy");
			String cacheStrategy = elem.getAttribute("cacheStrategy");
			DbEntity entity = new DbEntity(model, ClassUtils.classForName(entityClass), entityName, tableName);
			
			List<DbField> fields = ListUtils.list();
			Element[] fieldElements = this.elementsNamed(elem, "field");
			if(fieldElements == null || fieldElements.length == 0) {
				DbLogger.rdbms.warn("DbModelGroup : no field definitions of entity '" + entityName + "' in model '" + model.name() + "'");
			} else {
				for(Element fieldElem : fieldElements) {
					DbField field = parseField(entity, fieldElem);
					if(field != null)
						fields.add(field);
				}
			}
			
			entity._fields = new DbField[fields.size()];
			for(int i = 0; i < fields.size(); i++) {
				DbField field = fields.get(i);
				entity._fields[i] = field;
			}
			
			
			List<DbRelationship> joins = ListUtils.list();
			Element[] joinElements = this.elementsNamed(elem, "relationship");
			if(joinElements != null && joinElements.length > 0) {
				for(Element joinElem : joinElements) {
					DbRelationship join = parseRelationship(entity, joinElem);
					if(join != null)
						joins.add(join);
				}
			}
			
			entity._relationships = new DbRelationship[joins.size()];
			for(int i = 0; i < joins.size(); i++) {
				DbRelationship join = joins.get(i);
				entity._relationships[i] = join;
				DbLogger.rdbms_dev.debug("Relationship added to entity'" + entity.entityName() + "' " + join.toString());
			}
			
			Element pkGenElement = this.elementNamed(elem, "primary-key-generator");
			if(pkGenElement != null) {
				parsePkGenerator(entity, pkGenElement);
			} else {
				entity.setPkGenerator(new DbPrimaryKeyGenerator.SequenceTableKeyGenerator());
			}
			
			Element inheritanceElement = this.elementNamed(elem, "inheritance");
			if(inheritanceElement != null) {
				String type = inheritanceElement.getAttribute("type");
				String parentEntityName = inheritanceElement.getAttribute("parentEntity");
				String discriminatorField = inheritanceElement.getAttribute("discriminatorField");
				String discriminatorValue = inheritanceElement.getAttribute("discriminatorValue");
				if(!nullOrEmpty(type) && !nullOrEmpty(parentEntityName)) {
					Map<String, String> inheritance = MapUtils.map();
					inheritance.put("type", type);
					inheritance.put("parentEntityName", parentEntityName);
					if(!nullOrEmpty(discriminatorField))
						inheritance.put("discriminatorField", discriminatorField);
					if(!nullOrEmpty(discriminatorValue))
						inheritance.put("discriminatorValue", discriminatorValue);
					_inheritanceDefs.put(entityName, inheritance);
				}
			}
			
			if(DbEntity.OptimisticLock.equals(lockingStrategy))
				entity._lockingStrategy = DbEntity.LockingStrategy.Optimistic;
			if(DbEntity.PessimisticLock.equals(lockingStrategy))
				entity._lockingStrategy = DbEntity.LockingStrategy.Pessimistic;
			if(DbEntity.FileLock.equals(lockingStrategy))
				entity._lockingStrategy = DbEntity.LockingStrategy.File;
			
			DbEntity.CacheStrategy cache = DbEntity.CacheStrategy.Normal;
			if(cacheStrategy != null && cacheStrategy.length() > 0) {
				if(DbEntity.NoCache.equals(cacheStrategy))
					cache = DbEntity.CacheStrategy.None;
				if(DbEntity.NormalCache.equals(cacheStrategy))
					cache = DbEntity.CacheStrategy.Normal;
				if(DbEntity.StatisticalCache.equals(cacheStrategy))
					cache = DbEntity.CacheStrategy.Statistical;
				if(DbEntity.DistantFutureCache.equals(cacheStrategy))
					cache = DbEntity.CacheStrategy.DistantFuture;
			}
			entity._cacheStrategy = cache;
			
			if(entity.primaryKeyFields().size() > 1)
				entity._compositePrimaryKey = true;
			
			DbLogger.rdbms_dev.debug("Adding entity '" + entity.entityName() + "' to model '" + model.name() + "'");
			model.addEntity(entity.entityClass(), entity);
		}

	
		private void parsePkGenerator(DbEntity entity, Element element) {
			String className = element.getAttribute("class");
			if(nullOrEmpty(className))
				return;
			DbPrimaryKeyGenerator pkGenerator = (DbPrimaryKeyGenerator) ClassUtils.newInstance(className, DbPrimaryKeyGenerator.class.getName());
			if(pkGenerator == null) {
				DbLogger.rdbms.error("Could not instantiate primary key generator '" + className + "'");
				return;
			}
			DictionaryParser parser = new DictionaryParser();
			Map<String, Object> dict = null;
			try {
				dict = parser.parseDictionary(element);
			} catch (Exception e) {
				DbLogger.rdbms.warn("Could not read configuraion of primary key generator '" + className + "'");
			}
			pkGenerator.setConfig(dict);
			entity.setPkGenerator(pkGenerator);
		}

		private DbField parseField(DbEntity entity, Element fieldElem) {
			String columnName = fieldElem.getAttribute("name");
			String key = fieldElem.getAttribute("key");
			String valueClass = fieldElem.getAttribute("valueClass");
			String primaryKey = fieldElem.getAttribute("primaryKey");
			String readOnly = fieldElem.getAttribute("readOnly");
			String lock = fieldElem.getAttribute("lock");
			String dateFormat = fieldElem.getAttribute("dateFormat");
			
			boolean isPrimaryKey = (nullOrEmpty(primaryKey)) ? false : ("true".equalsIgnoreCase(primaryKey) ? true : false);
			boolean isReadOnly = (nullOrEmpty(readOnly)) ? false : ("true".equalsIgnoreCase(readOnly) ? true : false);
			boolean lockKey = (nullOrEmpty(lock)) ? false : ("true".equalsIgnoreCase(lock) ? true : false);
			
			if(nullOrEmpty(columnName)) {
				DbLogger.rdbms.warn("DbModelGroup : no column name in entity '" + entity.entityName() + "'");
				return null;
			}
			if(nullOrEmpty(key)) {
				DbLogger.rdbms.warn("DbModelGroup : no key in entity of column '" + columnName + "' in entity '" + entity.entityName() + "'");
			}
			if(nullOrEmpty(valueClass)) {
				DbLogger.rdbms.warn("DbModelGroup : no value class in entity of column '" + columnName + "' in entity '" + entity.entityName() + "'");
			}
			if(nullOrEmpty(dateFormat) && valueClass.equalsIgnoreCase("Date")) {
				DbLogger.rdbms.warn("DbModelGroup : no dateFormat attribute for Date class field '" + key + "' in entity '" + entity.entityName() + "'");
			}

			Class<?> validValueClass = validateValueClassName(valueClass);
			DbField field = new DbField(entity, columnName, key, validValueClass, isPrimaryKey);
			
			field._isReadOnly = isReadOnly;
			field._lockKey = lockKey;
			if(!StringUtils.nullOrEmpty(dateFormat))
				field._dateFormat = dateFormat;
						
			return field;
		}
		
		
		private DbRelationship parseRelationship(DbEntity entity, Element joinElem) {
			String key = joinElem.getAttribute("key");
			String toMany = joinElem.getAttribute("toMany");
			String prefetch = joinElem.getAttribute("prefetch");
			String destEntityName = joinElem.getAttribute("destinationEntityName");
			String sourceKey = joinElem.getAttribute("sourceKey");
			String destKey = joinElem.getAttribute("destinationKey");
			String own = joinElem.getAttribute("ownsDestination");
			String cache = joinElem.getAttribute("cacheDestination");
			boolean isToMany = (nullOrEmpty(toMany)) ? true : ("true".equalsIgnoreCase(toMany) ? true : false);
			boolean shouldPrefetch = (nullOrEmpty(prefetch)) ? false : ("true".equalsIgnoreCase(prefetch) ? true : false);
			boolean ownsDestination = (nullOrEmpty(own)) ? false : ("true".equalsIgnoreCase(own) ? true : false);
			boolean cacheDestination = (nullOrEmpty(cache)) ? true : ("false".equals(cache) ? false : true);

			if(nullOrEmpty(key)) {
				DbLogger.rdbms.warn("DbModelGroup : no key in relationship '" + key + "' in entity '" + entity.entityName() + "'");
				return null;
			}
			if(nullOrEmpty(destEntityName)) {
				DbLogger.rdbms.warn("DbModelGroup : no destination entity name in relationship '" + key + "' in entity '" + entity.entityName() + "'");
				return null;
			}
			if(nullOrEmpty(sourceKey)) {
				DbLogger.rdbms.warn("DbModelGroup : no source key entity name in relationship '" + key + "' in entity '" + entity.entityName() + "'");
				return null;
			}
			if(nullOrEmpty(destKey)) {
				DbLogger.rdbms.warn("DbModelGroup : no source key entity name in relationship '" + key + "' in entity '" + entity.entityName() + "'");
				return null;
			}

			DbRelationship join = new DbRelationship(entity, destEntityName, key, sourceKey, destKey, isToMany, ownsDestination);
			join._shouldPrefetch = shouldPrefetch;
			join._cacheDestination = cacheDestination;
			return join;
		}



		private Class<?> validateValueClassName(String valueClass) {
			for(int i = DbField.AffordableValueClassNames.length - 1; i >= 0; i--) {
				String className = DbField.AffordableValueClassNames[i];
				if(className.equalsIgnoreCase(valueClass))
					return DbField.AffordableValueClasses[i];
			}
			return java.lang.String.class;
		}

		private void paserConnectionInfo(DbModel model, Element connElem) {
			String driverClass = connElem.getAttribute("driver-class");
			String username = connElem.getAttribute("username");
			String password = connElem.getAttribute("password");
			String url = connElem.getAttribute("url");
			if(!nullOrEmpty(driverClass) && !nullOrEmpty(username) && !nullOrEmpty(password) && !nullOrEmpty(url)) {
				DbConnectionInfo info = new DbConnectionInfo();
				info.setDriverClass(driverClass);
				info.setUsername(username);
				info.setPassword(password);
				info.setUrl(url);
				model._connectionInfo = info;
				
				Properties props = System.getProperties();
				if(props != null && props.size() > 0) {
					Properties defaults = new Properties(props);
					info.setProperties(defaults);
				}
				Element propElems[] = elementsNamed(connElem, "property");
				if(propElems != null && propElems.length > 0) {
					for(Element propElem : propElems) {
						parseConnectionInfoProperty(info, propElem);
					}
				}
			}
		}
		
		private void parseConnectionInfoProperty(DbConnectionInfo info, Element propElem) {
			String name = propElem.getAttribute("name");
			String value = propElem.getAttribute("value");
			if(!nullOrEmpty(name) && !nullOrEmpty(value)) {
				Properties props = info.properties();
				if(props == null) {
					props = new Properties();
					info.setProperties(props);
				}
				props.put(name, value);
			}
		}

		private boolean nullOrEmpty(String str) {
			return (str == null || str.length() == 0);
		}
		
		private boolean getBooleanAttribute(Element elem, String attrName, boolean defaultValue) {
			String attr = elem.getAttribute(attrName);
			if(attr == null || attr.isEmpty())
				return defaultValue;
			return ("true".equalsIgnoreCase(attr) || "yes".equalsIgnoreCase(attr)) ? true : false;
		}
		private int getIntAttribute(Element elem, String attrName, int defaultValue) {
			String attr = elem.getAttribute(attrName);
			if(attr == null || attr.isEmpty())
				return defaultValue;
			int value = defaultValue;
			try {
				value = Integer.parseInt(attr);
			} catch (Exception ignore) {}
			return value;
		}
		private long getLongAttribute(Element elem, String attrName, long defaultValue) {
			String attr = elem.getAttribute(attrName);
			if(attr == null || attr.isEmpty())
				return defaultValue;
			long value = defaultValue;
			try {
				value = Long.parseLong(attr);
			} catch (Exception ignore) {}
			return value;
		}
		
		
		private void makeInheritance() {
			for(String entityName : _inheritanceDefs.keySet()) {
				Map<String, String> inheritance = _inheritanceDefs.get(entityName);
				DbEntity entity = DbModelGroup.defaultGroup().entityNamed(entityName);
				DbEntity parentEntity = DbModelGroup.defaultGroup().entityNamed(inheritance.get("parentEntityName"));
				if(entity != null && parentEntity != null) {
					DbInheritance inherit = null;
					if("singleTable".equals(inheritance.get("type")))
						inherit = new DbInheritance(DbInheritance.InheritanceType.SingleTable, parentEntity,
								inheritance.get("discriminatorField"), inheritance.get("discriminatorValue"));
					if("tablePerClass".equals(inheritance.get("type")))
						inherit = new DbInheritance(DbInheritance.InheritanceType.TablePerClass, parentEntity);
					entity._inheritance = inherit;
				}
			}
		}

	}

	public static class DictionaryParser {
		
		public static final String DICT_TYPE = "dict";
		public static final String ARRAY_TYPE = "array";
		public static final String STRING_TYPE = "string";
		public static final String NUMBER_TYPE = "number";
		public static final String DATE_TYPE = "date";
		public static final String BOOLEAN_TYPE = "boolean";
		private static final String LONG_DATE_FORMAT_STRING = "yyyy/MM/dd HH:mm:SS";
		private static final String SHORT_DATE_FORMAT_STRING = "yyyy/MM/dd";
		private static final DateFormat LONG_DATE_FORMAT = new SimpleDateFormat(LONG_DATE_FORMAT_STRING);
		private static final DateFormat SHORT_DATE_FORMAT = new SimpleDateFormat(SHORT_DATE_FORMAT_STRING);

		public Map<String, Object> parseDictionary(Element parent) throws Exception {
			Map<String, Object> keyValues = MapUtils.map();
			
			Element elements[] = XMLUtil.getAllChildren(parent, null);
			int size = elements.length;
			for(int i = 0; i < size; ) {
				Element keyElement = elements[i];
				Element valueElement = elements[i+1];
				if(keyElement.getNodeName().equals("key") && isValidValueElement(valueElement)) {
					String key = XMLUtil.getText(keyElement, null);
					Object value = objectValue(valueElement);
					if(key != null && value != null) {
						keyValues.put(key, value);
					}
				}
				i += 2;
			}
			return keyValues;
		}
		

		private Object objectValue(Element element) throws Exception {
			String type = element.getNodeName();
			if(DICT_TYPE.equals(type)) {
				return parseDictionary(element);
			} else if(ARRAY_TYPE.equals(type)) {
				return parseArray(element);
			} else if(STRING_TYPE.equals(type)) {
				return XMLUtil.getText(element, null);
			} else if(NUMBER_TYPE.equals(type)) {
				return parseNumber(element);
			} else if(DATE_TYPE.equals(type)) {
				return parseDate(element);
			} else if(BOOLEAN_TYPE.equals(type)) {
				return parseBoolean(element);
			}
			return null;
		}

		private List<Object> parseArray(Element element) throws Exception {
			Element elements[] = XMLUtil.getAllChildren(element, null);
			List<Object> list = ListUtils.list();
			int size = elements.length;
			for(int i = 0; i < size; i++) {
				Object value = this.objectValue(elements[i]);
				if(value != null) {
					list.add(value);
				}
			}
			return list;
		}
		
		private String numberString(String string) {
			StringBuffer b = new StringBuffer();
			for(int i = 0; i < string.length(); i++) {
				char ch = string.charAt(i);
				if(CharacterSet.numberCharacterSet.contains(ch))
					b.append(ch);
			}
			return b.toString();
		}
		
		private Double toDouble(String string, double defaultValue) {
			if(string == null)
				return new Double(defaultValue);
			String s = string.trim();
			boolean minus = false;
			if(s.charAt(0) == '-') {
				minus = true;
				s = s.substring(1);
			}
			String numberStr = numberString(s);
			if(numberStr.length() > 0) {
				double val = Double.parseDouble(numberStr);
				if(minus)
					return new Double(-val);
				else
					return new Double(val);
			}
			return new Double(defaultValue);
		}

		private Number parseNumber(Element element) {
			String value = XMLUtil.getText(element, null);
			if(value == null)
				return null;
			return toDouble(value, 0.0);
		}
		
		private Date parseDate(Element element) throws ParseException {
			String value = XMLUtil.getText(element, null);
			if(value == null) {
				return null;
			}
			Date date = null;
			if(value.length() == LONG_DATE_FORMAT_STRING.length())
				date = LONG_DATE_FORMAT.parse(value);
			else if(value.length() == SHORT_DATE_FORMAT_STRING.length())
				date = SHORT_DATE_FORMAT.parse(value);
			return date;
		}
		
		private Boolean parseBoolean(Element element) {
			String value = XMLUtil.getText(element, null);
			if(value == null) {
				return null;
			}
			if("true".equalsIgnoreCase(value) || "yes".equalsIgnoreCase(value))
				return new Boolean(true);
			else if("false".equalsIgnoreCase(value) || "no".equalsIgnoreCase(value))
				return new Boolean(false);
			return null;
		}

		private boolean isValidValueElement(Element element) {
			String type = element.getNodeName();
			if(DICT_TYPE.equals(type) || 
					ARRAY_TYPE.equals(type) || 
					STRING_TYPE.equals(type) || 
					NUMBER_TYPE.equals(type) || 
					DATE_TYPE.equals(type) || 
					BOOLEAN_TYPE.equals(type))
				return true;
			return false;
		}

	}
}
