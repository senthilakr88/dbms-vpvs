package edu.buffalo.cse562.logicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import edu.buffalo.cse562.logger.logManager;
import net.sf.jsqlparser.expression.Expression;


public class components {

	logManager lg;
	Map<String,ArrayList<String>> tableMap;
	ArrayList<String> projectStmt;
	Expression whereClause;
	
	public components () {
		tableMap = new HashMap<String,ArrayList<String>>();
		lg = new logManager();
	}
	
	public void initializeNewStatement() {
		projectStmt = new ArrayList<String>();
		
	}
	
	public void addColsToTable (String table, ArrayList<String> cols) {
		if(tableMap.containsKey(table)) {
			tableMap.remove(table);
			tableMap.put(table, cols);
		} else {
			tableMap.put(table, cols);
		}
	}
	
	public void addProjectStmts (List list) {
		projectStmt.addAll(list);
	}
	
	public void addWhereConditions(Expression where) {
		whereClause = where;
	}
	
	public String toString() {
		StringBuffer toPrint = new StringBuffer();
		toPrint.append("PROJECT [("	+ projectStmt + ")]\n");
		toPrint.append("SELECT ["+whereClause+"\n");
		for (Map.Entry<String, ArrayList<String>> entry : tableMap.entrySet()){
		toPrint.append("SCAN ["	+ entry.getKey() + "(" +entry.getValue() +")]");
		}
		return toPrint.toString();
	}

	
	
}
