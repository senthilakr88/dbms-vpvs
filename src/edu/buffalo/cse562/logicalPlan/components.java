package edu.buffalo.cse562.logicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.FileScanOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;


public class components {

	logManager lg;
	Map<String,ArrayList<String>> tableMap;
	ArrayList<String> projectStmt;
	Expression whereClause;
	String tableDir;
	FromItem tableName;
	
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

	public void executePhysicalPlan() {
		Table t = (Table) tableName;
		FileScanOperator f = new FileScanOperator(t.getName(),tableDir,tableMap);
		System.out.println(f.readOneTuple());
		lg.logger.log(Level.INFO, new FileScanOperator(t.getName(),tableDir,tableMap).readOneTuple().toString());
		System.out.println(f.readOneTuple());
		
	}

	public void setTableDirectory(String tableDir) {
		this.tableDir=tableDir;
		
		
	}

	public void setFromItems(FromItem fromItem) {
		tableName = fromItem;
		
	}

	
	
}
