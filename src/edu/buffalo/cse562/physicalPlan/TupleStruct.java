package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import edu.buffalo.cse562.logicalPlan.buildindexer.Row;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.Datum.dDate;
import edu.buffalo.cse562.structure.Datum.dDecimal;
import edu.buffalo.cse562.structure.Datum.dLong;
import edu.buffalo.cse562.structure.Datum.dString;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class TupleStruct {

	static List<String> tupleTableMap;
	static Column[] tupleTableColMap;
	static Integer[] tuplecolTypeMap;
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

	public static Column[] getTupleTableColMap() {
		return tupleTableColMap;
	}

	public static Integer[] getTupleTableColTypeMap() {
		return tuplecolTypeMap;
	}

	public static void setTupleTableColMap(Datum[] element) {
		int index;
		tupleTableColMap = new Column[element.length];
		tuplecolTypeMap = new Integer[element.length];
		for (index = 0; index < element.length; index++) {
			// System.out.println("Tuple Struct Building on index :: " + index);
			Datum row = (Datum) element[index];
			if (row instanceof dLong) {
				tuplecolTypeMap[index] = 0;
			} else if (row instanceof dDecimal) {
				tuplecolTypeMap[index] = 1;
			} else if (row instanceof dString) {
				tuplecolTypeMap[index] = 2;
			} else if (row instanceof dDate) {
				tuplecolTypeMap[index] = 3;
			}
			tupleTableColMap[index] = row.getColumn();
		}
		// System.out.println("Tuple Struct moving out of tuple col map :: " +
		// index);

	}

	public static void setTupleTableMap(Datum[] t) {
		int index;
		tupleTableMap = new ArrayList<String>(t.length);
		// System.out.println("---------------------------------------------------------");
		for (index = 0; index < t.length; index++) {
			Datum row = (Datum) t[index];
			if (row.getColumn().getColumnName() != null) {
				Table tableName = row.getColumn().getTable();

				String datumColumn = row.getColumn().getColumnName()
						.toLowerCase();
				// System.out.println(row.getColumn().getColumnName());
				if (tableName != null) {
					String alias = tableName.getAlias();
					String tableNameStr = tableName.getName();
					if (alias != null) {
						// System.out.println("alias "+ " :: "+
						// alias+" datumColumn :: "+datumColumn);
						tupleTableMap.add(alias.toLowerCase() + "."
								+ datumColumn);
					} else if (joinCondition && tableNameStr != null) {
						// System.out.println("tableNameStr "+ " :: "+
						// tableNameStr+" datumColumn :: "+datumColumn);
						tupleTableMap.add(tableNameStr.toLowerCase() + "." + datumColumn);
					} else {
						// System.out.println(tableName + " :: " + datumColumn);
						tupleTableMap.add(datumColumn);
					}
				} else {
					// System.out.println(datumColumn);
					tupleTableMap.add(datumColumn);
				}
			} else {
				tupleTableMap.add(null);
			}

		}
		// System.out.println("TupleStruct" + tupleTableMap);
		// System.out.println("---------------------------------------------------------");
	}

	public static List<String> getTupleTableMap() {
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
			System.out
					.println("Index of datum not identified. Throwing error in TupleStruct");
		}
		return key;
	}

	public static int getColIndex(Datum[] tuple, Column column) {
		int index = -1;
		List<String> tupleTableMap = getTupleTableMap();
//		System.out.println(tupleTableMap);
		String columnName = column.getWholeColumnName().toLowerCase();
//		System.out.println(columnName);
		if (tupleTableMap.contains(columnName)) {
			index = tupleTableMap.indexOf(columnName);
		}
		return index;
	}

	public static int compare(Object leftKey2, Object rightKey2) {
		if (leftKey2 instanceof String && rightKey2 instanceof String) {
			// System.out.println((String) leftKey2 + " :: " + (String)
			// rightKey2 + "::" + ((String) leftKey2).compareTo((String)
			// rightKey2));
			return ((String) leftKey2).compareTo((String) rightKey2);
		} else if (leftKey2 instanceof Double && rightKey2 instanceof Double) {
			return ((Double) leftKey2).compareTo((Double) rightKey2);
		} else if (leftKey2 instanceof Long && rightKey2 instanceof Long) {
			return ((Long) leftKey2).compareTo((Long) rightKey2);
		} else if (leftKey2 instanceof Date && rightKey2 instanceof Date) {
			return ((Date) leftKey2).compareTo((Date) rightKey2);
		} else {
			System.out.println("Unindentified type in TupleStruct :: Compare");
			return -2;
		}
	}

	public static int getCompareValue(Object t1, Object t2, boolean asc) {
		if (t1 instanceof Long) {
			Long value1 = (Long) t1;
			Long value2 = (Long) t2;
			int comp = value1.compareTo(value2);
			// System.out.println(comp);
			return compHelper(comp, asc);
		} else if (t1 instanceof Double) {
			Double value1 = (Double) t1;
			Double value2 = (Double) t2;
			int comp = value1.compareTo(value2);
			// System.out.println("compare value :: " +comp);
			return compHelper(comp, asc);
		} else if (t1 instanceof String) {
			String value1 = (String) t1;
			String value2 = (String) t2;
			int comp = value1.compareTo(value2);
			// System.out.println("compare value :: " +comp);
			return compHelper(comp, asc);
		} else if (t1 instanceof Date) {
			Date value1 = (Date) t1;
			Date value2 = (Date) t2;
			int comp = value1.compareTo(value2);
			return compHelper(comp, asc);
		}
		if (t1 instanceof dLong) {
			Long value1 = ((dLong) t1).getValue();
			Long value2 = ((dLong) t2).getValue();
			int comp = value1.compareTo(value2);
			// System.out.println(comp);
			return compHelper(comp, asc);
		} else if (t1 instanceof dDecimal) {
			Double value1 = ((dDecimal) t1).getValue();
			Double value2 = ((dDecimal) t2).getValue();
			int comp = value1.compareTo(value2);
			// System.out.println("compare value :: " +comp);
			return compHelper(comp, asc);
		} else if (t1 instanceof dString) {
			String value1 = ((dString) t1).getValue();
			String value2 = ((dString) t2).getValue();
			// System.out.println("value 1 :: " + value1 + " value2 :: " +
			// value2);
			int comp = value1.compareTo(value2);
			// System.out.println("compare value :: " +comp);
			return compHelper(comp, asc);
		} else if (t1 instanceof dDate) {
			Date value1 = ((dDate) t1).getValue();
			Date value2 = ((dDate) t2).getValue();
			int comp = value1.compareTo(value2);
			return compHelper(comp, asc);
		} else {
			return -2;
		}

	}

	public static int getCompareValue(Datum t1, Datum t2, boolean asc) {
		int comp = t1.compareTo(t2);
		// System.out.println(comp);
		return compHelper(comp, asc);
	}

	public static int getCompareValue(Datum[] t1, Datum[] t2) {
		int comp;
//		System.out.println("-----------------------------");
//		printTuple(t1);
//		printTuple(t2);
//		System.out.println("-----------------------------");
		for (int i = 0; i < t1.length; i++) {
			if (i >= t2.length) {
				return 0;
			}
			comp = t1[i].compareTo(t2[i]);
			if (comp != 0) {
				return comp;
			}
		}
		return 0;

	}

	private static void printTuple(Datum[] row) {
		Boolean first = true;
		if (row != null && row.length != 0) {
			for (Datum col : row) {
				if (!first)
					System.out.print("|" + col);
				else {
					System.out.print(col);
					first = false;
				}
			}
			System.out.println();
		}
	}
	
	private static int compHelper(int comp, boolean asc) {
		if (comp == 0) {
			return comp;
		} else if (asc) {
			return comp;
		} else {
			if (comp == -1)
				return 1;
			else
				return -1;
		}
	}
	
	
	public static List<String> getColString(List<Column> t1) {
		List<String> colStr = new ArrayList<String>();
//		System.out.println("getColString :: " + t1);
		for(int i=0; i < t1.size();i++) {
			colStr.add(t1.get(i).getColumnName());
		}
//		System.out.println("colStr :: "+colStr);
		return colStr;
	}
	
	public static List<Integer> getColPositions(List<String> t1, List<String> t2) {
		List<Integer> colPos = new ArrayList<Integer>();
//		System.out.println(t1);
//		System.out.println(t2);
		for(int i=0; i < t2.size();i++) {
			colPos.add(t1.indexOf(t2.get(i)));
		}	
//		System.out.println(colPos);
		return colPos;
	}
	
	public static Datum[] getDatum(Datum[] datum, List<Integer> col) {
		Datum[] newDatum = new Datum[col.size()];
		for(int i=0;i<col.size();i++){
			newDatum[i] = datum[col.get(i)]; 
		}
		return newDatum;
	}

}
