package rdbms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import rdbms.command.SQLGenerationContext;
import core.util.FieldAccess;

public class DbSortOrdering {

	public enum Direction {Ascending, Descending, CaseInsensitiveAsending, CaseInsensitiveDescending};
	
	String _key;
	Direction _direction;
	
	public DbSortOrdering(String key, Direction direction) {
		this._key = key;
		this._direction = direction;
	}
	
	public String key() {
		return _key;
	}
	
	public Direction direction() {
		return _direction;
	}

	public boolean isAscending() {
		return (_direction == Direction.Ascending || _direction == Direction.CaseInsensitiveAsending);
	}
	
	public boolean isDescending() {
		return (_direction == Direction.Descending || _direction == Direction.CaseInsensitiveDescending);
	}
	
	public boolean isCaseInsensitive() {
		return (_direction == Direction.CaseInsensitiveAsending || _direction == Direction.CaseInsensitiveDescending);
	}
	
	public String generateString(DbEntity entity) {
		DbField field = entity.fieldNamed(key());
		if(isDescending())
			return field.columnName() + " DESC";
		return field.columnName();
	}
	
	public String generateStringInContext(DbEntity entity,
			SQLGenerationContext ctx) {
		DbField field = entity.fieldNamed(key());
		if(isDescending())
			return ctx.columnNameForField(field, entity) + " DESC";
		return ctx.columnNameForField(field, entity);
	}

	public List<?> sortedListUsingSortOrderings(List<?> list, List<DbSortOrdering> orderings) {
		List newList = new ArrayList(list);
		sortUsingSortOrderings(newList, orderings);
		return newList;
	}
	public static void sortUsingSortOrderings(List<?> list, List<DbSortOrdering> orderings) {
		SOComparator comp = new SOComparator(orderings);
		Collections.sort(list, comp);
	}
	
	static class SOComparator implements Comparator {

		private List<DbSortOrdering> _orderings;

		SOComparator(List<DbSortOrdering> orderings) {
			_orderings = orderings;
		}
		
		private int compareUsingSortOrdering(Object o1, Object o2, DbSortOrdering ordering) {
			if(o1 == null && o2 == null)
				return 0;
			if(o1 != null && o2 == null)
				return ordering.isAscending() ? -1 : 1;
			if(o1 == null && o2 != null)
				return ordering.isAscending() ? 1 : -1;
			
			Object value1 = FieldAccess.Util.getValueForKey(o1, ordering.key());
			Object value2 = FieldAccess.Util.getValueForKey(o2, ordering.key());
			return ((Comparable) value1).compareTo((Comparable) value2);
		}
		
		@Override
		public int compare(Object o1, Object o2) {
			for(DbSortOrdering ordering : _orderings) {
				int result = compareUsingSortOrdering(o1, o2, ordering);
				if(result != 0)
					return result;
			}
			return 0;
		}
		
	}


}
