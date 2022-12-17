package rdbms;

public interface DbValidation {

	public abstract void validateForSave(DbContext uniContext) throws DbValidationException;
	public abstract void validateForDelete(DbContext uniContext) throws DbValidationException;

}
