package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;

public class TupleStruct {

	static List<String> tupleTableMap;
	static boolean joinCondition;
	
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
			String alias = row.getColumn().getTable().getAlias();
			String datumColumn = row.getColumn().getColumnName().toLowerCase();
			if(alias !=null) {
				tupleTableMap.add(alias+"."+datumColumn);
			} else if(joinCondition) {
				String tableName = row.getColumn().getTable().getName();
				tupleTableMap.add(tableName+"."+datumColumn);
			} else {
				tupleTableMap.add(datumColumn);
			}
				
		}
	}
	
	public static List<String> getTupleTableMap () {
		return tupleTableMap;
	}
	
}
