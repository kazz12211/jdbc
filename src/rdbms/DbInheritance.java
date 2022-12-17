package rdbms;


public class DbInheritance {
	public enum InheritanceType {
		SingleTable, TablePerClass
	}

	InheritanceType _type;
	DbEntity _parentEntity;
	DbField _discriminateField;
	String _discriminateValue;
	JoinTable _joinTable;

	public DbInheritance(InheritanceType type, DbEntity parentEntity) {
		_type = type;
		_parentEntity = parentEntity;
	}

	public DbInheritance(InheritanceType type, DbEntity parentEntity, String discriminateFieldKey, String discriminateValue) {
		this(type, parentEntity);
		_discriminateField = parentEntity.fieldNamed(discriminateFieldKey);
		_discriminateValue = discriminateValue;
	}

	public InheritanceType type() {
		return _type;
	}

	public DbEntity parentEntity() {
		return _parentEntity;
	}

	public DbField discriminateField() {
		return _discriminateField;
	}

	public String discriminateValue() {
		return _discriminateValue;
	}

	public boolean isSingleTableInheritance() {
		return _type == InheritanceType.SingleTable;
	}
	
	public JoinTable joinTable() {
		return _joinTable;
	}
	
	public String toString() {
		return "{type=" + (_type == InheritanceType.SingleTable ? "SingleTable" : "TablePerClass") + "; parentEntity=" + _parentEntity.entityName() + 
				"; discriminateField=" + _discriminateField + "; discriminateValue=" + _discriminateValue + "; joinTable=" + _joinTable + "}";
	}
	
	public DbPredicate predicate() {
		if(isSingleTableInheritance()) {
			DbPredicate p = new DbPredicate.KeyValue(this.discriminateField().key(), this.discriminateValue());
			return p;
		}
		return null;
	}

	
	public class JoinTable {
		String _filename;
		String _sourceField;
		String _destinationField;
		
		public JoinTable(String filename, String sourceField, String destinationField) {
			_filename = filename;
			_sourceField = sourceField;
			_destinationField = destinationField;
		}
		
		public String filename() {
			return _filename;
		}
		public String sourceField() {
			return _sourceField;
		}
		public String destinationField() {
			return _destinationField;
		}
		@Override
		public String toString() {
			return "{filename=" + _filename + "; sourceField=" + _sourceField + "; destinationField=" + _destinationField + "}";
		}
	}

}
