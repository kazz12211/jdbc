package rdbms.object;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import rdbms.DbContext;
import rdbms.DbQuery;
import rdbms.DbRelationship;

public class DbFaultingList<E> extends ArrayList<E> {
	private static final long serialVersionUID = 1L;
	private boolean _isFault = false;
	private Object _owner;
	private DbRelationship _relationship;
	
	public boolean isFault() {
		return _isFault;
	}
	public void setFault(boolean isFault) {
		_isFault = isFault;
	}

	private void loadStoredValues() {
		DbContext ctx = DbContext.get();
		DbQuery processor = ctx.queryProcessorForEntity(_relationship.entity());
		List objects;
		objects = processor.loadObjectsInRelationship(_owner, _relationship);
		super.clear();
		super.addAll(objects);
		_isFault = false;
	}
	
	public DbFaultingList(Object owner, DbRelationship relationship) {
		super();
		this._owner = owner;
		this._relationship = relationship;
		this._isFault = true;
	}

	public Object owner() {
		return _owner;
	}
	public void setOwner(Object owner) {
		this._owner = owner;
	}
	
	public DbRelationship relationship() {
		return _relationship;
	}
	
	public void setRelationship(DbRelationship relationship) {
		this._relationship = relationship;
	}
	
	@Override
	public boolean contains(Object arg0) {
		if(_isFault)
			loadStoredValues();
		return super.contains(arg0);
	}

	@Override
	public int indexOf(Object arg0) {
		if(_isFault)
			loadStoredValues();
		return super.indexOf(arg0);
	}

	@Override
	public int lastIndexOf(Object arg0) {
		if(_isFault)
			loadStoredValues();
		return super.lastIndexOf(arg0);
	}

	@Override
	public int size() {
		if(_isFault)
			loadStoredValues();
		return super.size();
	}

	@Override
	public Object[] toArray() {
		if(_isFault)
			loadStoredValues();
		return super.toArray();
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
		if(_isFault)
			loadStoredValues();
		return super.toArray(arg0);
	}

	@Override
	public Iterator<E> iterator() {
		if(_isFault)
			loadStoredValues();
		return super.iterator();
	}

	@Override
	public ListIterator<E> listIterator() {
		if(_isFault)
			loadStoredValues();
		return super.listIterator();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		if(_isFault)
			loadStoredValues();
		return super.listIterator(index);
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		if(_isFault)
			loadStoredValues();
		return super.subList(fromIndex, toIndex);
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
		if(_isFault)
			loadStoredValues();
		return super.containsAll(arg0);
	}

	public void turnIntoFault() {
		super.clear();
		_isFault = true;
	}

}
