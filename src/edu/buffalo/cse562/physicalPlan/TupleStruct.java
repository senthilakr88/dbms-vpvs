package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class TupleStruct {

	static List<String> tupleTableMap;
	static boolean joinCondition;
	static boolean nestedCondition = false;
	
	public static boolean isNestedCondition() {
		return nestedCondition;
	}

	public static void setNestedCondition(boolean nestedCondition) {
		TupleStruct.nestedCondition = nestedCondition;
	}

	public static Boolean getJoinCondition() {
		return joinCondition;
	}

	public static void setJoinCondition(Boolean joinCondition) {
		TupleStruct.joinCondition = joinCondition;
	}

	public static void setTupleTableMap(Datum[] t) {
		int index;
		tupleTableMap = new ArrayList<String>(t.length);
		for(index = 0;index < t.length;index++) {
			Datum row = (Datum) t[index];
			Table tableName = row.getColumn().getTable();
			
			String datumColumn = row.getColumn().getColumnName().toLowerCase();
			if(tableName != null) {
				String alias = tableName.getAlias();
				if(alias !=null) {
					tupleTableMap.add(alias.toLowerCase()+"."+datumColumn);
				} else if(joinCondition) {
					tupleTableMap.add(tableName.getName()+"."+datumColumn);
				} else {
					tupleTableMap.add(datumColumn);
				}
			} else {
				tupleTableMap.add(datumColumn);
			}
			
				
		}
	}
	
	public static List<String> getTupleTableMap () {
		return tupleTableMap;
	}
	
	public static Object getKey(Datum[] tuple, int colIndex) {
		Object key = null;
		if (colIndex != -1) {
			Datum row = tuple[colIndex];
			if (row instanceof Datum.dLong) {
				key = ((Datum.dLong) row).getValue();

			} else if (row instanceof Datum.dDate) {
				key = ((Datum.dDate) row).getValue();

			} else if (row instanceof Datum.dString) {
				key = ((Datum.dString) row).getValue();

			} else if (row instanceof Datum.dDecimal) {
				key = ((Datum.dDecimal) row).getValue();
			}
		} else {
			System.out.println("Index of datum not identified. Throwing error in TupleStruct");
		}
		return key;
	}
	
	public static int getColIndex(Datum[] tuple, Column column) {
		int index = -1;
		List<String> tupleTableMap = getTupleTableMap();
		//System.out.println(tupleTableMap);
		String columnName = column.getWholeColumnName().toLowerCase();
		if (tupleTableMap.contains(columnName)) {
			index = tupleTableMap.indexOf(columnName);
		}
		return index;
	}
	
	public static int compare(Object leftKey2, Object rightKey2) {
		if (leftKey2 instanceof String && rightKey2 instanceof String) {
			//System.out.println((String) leftKey2 + " :: " + (String) rightKey2 + "::" + ((String) leftKey2).compareTo((String) rightKey2));
			return ((String) leftKey2).compareTo((String) rightKey2);
		} else if (leftKey2 instanceof Double && rightKey2 instanceof Double) {
			return ((Double) leftKey2).compareTo((Double) rightKey2);
		} else if (leftKey2 instanceof Long && rightKey2 instanceof Long) {
			return ((Long) leftKey2).compareTo((Long) rightKey2);
		} else if (leftKey2 instanceof Date && rightKey2 instanceof Date) {
			return ((Date) leftKey2).compareTo((Date) rightKey2);
		} else {
			System.out
					.println("Unindentified type in TupleStruct :: Compare");
			return -2;
		}
	}
}
