package edu.buffalo.cse562.logicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.AggregateOperator;
import edu.buffalo.cse562.physicalPlan.Datum;
import edu.buffalo.cse562.physicalPlan.FileScanOperator;
import edu.buffalo.cse562.physicalPlan.JoinOperator;
import edu.buffalo.cse562.physicalPlan.Operator;
import edu.buffalo.cse562.physicalPlan.ProjectionOperator;
import edu.buffalo.cse562.physicalPlan.SelectionOperator;
import edu.buffalo.cse562.physicalPlan.Tuple;

public class components {

	logManager lg;
	List<Column> tableMap;
	Map<String, ArrayList<String>> tableColTypeMap;
	ArrayList<SelectExpressionItem> projectStmt;
	ArrayList tableJoins;
	Expression whereClause;
	String tableDir;
	FromItem tableName;
	SelectBody selectBody;
	

	public components() {

		tableMap = new ArrayList<Column>();
		tableColTypeMap = new HashMap<String, ArrayList<String>>();
		lg = new logManager();
	}

	public void initializeNewStatement() {
		projectStmt = new ArrayList<SelectExpressionItem>();

	}

	public void addProjectStmts(List<SelectExpressionItem> list) {
		projectStmt.addAll(list);
	}

	public void addWhereConditions(Expression where) {
		whereClause = where;
	}

	public void setSelectBody(SelectBody selectBody) {
		this.selectBody = selectBody;
	}

	public String toString() {
		StringBuffer toPrint = new StringBuffer();
		toPrint.append("PROJECT [(" + projectStmt + ")]\n");
		toPrint.append("SELECT [" + whereClause + "\n");
		// for (Map.Entry<String, ArrayList<String>> entry :
		// tableMap.entrySet()) {
		// toPrint.append("SCAN [" + entry.getKey() + "(" + entry.getValue()
		// + ")]");
		// }
		return toPrint.toString();
	}

	public void executePhysicalPlan() {
		Table table = (Table) tableName;
		Operator oper = new FileScanOperator(table, tableDir, tableMap,
				tableColTypeMap);

		if (tableJoins != null) {
			Iterator joinIte = tableJoins.iterator();
			while (joinIte.hasNext()) {
				Join joinTable = (Join) joinIte.next();
				Operator rightOper = new FileScanOperator(
						(Table) joinTable.getRightItem(), tableDir, tableMap,
						tableColTypeMap);
				oper = new JoinOperator(oper, rightOper,
						joinTable.getOnExpression());
				
			}
		}

		if (!whereClause.equals(null)) {
			oper = new SelectionOperator(oper, whereClause);

		}

		if (((PlainSelect) selectBody).getGroupByColumnReferences() != null) {
			// Aggregate computation.
			oper.resetStream();
			oper = new AggregateOperator(oper, selectBody, tableMap);
			Datum[] test = oper.readOneTuple();
			test = oper.readOneTuple();
			test = oper.readOneTuple();
			test = oper.readOneTuple();
			System.out.println("PRINTING TUPLE FROM AGGREGATE OPERATOR");
			printTuple(test);
		}
		
		// Projection computation
		oper = new ProjectionOperator(oper, projectStmt);
		Datum[] t = oper.readOneTuple();
		while (t != null) {
			printTuple(t);
			t = oper.readOneTuple();
		}

	}
	


	private void printTuple(Datum[] row) {
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

	public void setTableDirectory(String tableDir) {
		this.tableDir = tableDir;

	}
	
	public void setFromItems(FromItem fromItem) {
		tableName = fromItem;

	}

	public void addColsTypeToTable(String table,
			ArrayList<String> columnTypeList) {
		if (tableColTypeMap.containsKey(table)) {
			tableColTypeMap.remove(table);
			tableColTypeMap.put(table, columnTypeList);
		} else {
			tableColTypeMap.put(table, columnTypeList);
		}

	}

	public void addColsToTable(ArrayList<Column> columnNameList) {
		tableMap.addAll(columnNameList);

	}

	public void addJoins(List joins) {
		this.tableJoins = (ArrayList) joins;

	}

}
