package edu.buffalo.cse562.logicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.AggregateOperator;
import edu.buffalo.cse562.physicalPlan.BNLJoinOperator;
import edu.buffalo.cse562.physicalPlan.Datum;
import edu.buffalo.cse562.physicalPlan.ExternalSort;
import edu.buffalo.cse562.physicalPlan.FileScanOperator;
import edu.buffalo.cse562.physicalPlan.FromItemParser;
import edu.buffalo.cse562.physicalPlan.GroupbyOperator;
import edu.buffalo.cse562.physicalPlan.HHJoinOperator;
import edu.buffalo.cse562.physicalPlan.LimitOperator;
import edu.buffalo.cse562.physicalPlan.Operator;
import edu.buffalo.cse562.physicalPlan.OrderByOperator;
import edu.buffalo.cse562.physicalPlan.ProjectionOperator;
import edu.buffalo.cse562.physicalPlan.SelectionOperator;
import edu.buffalo.cse562.physicalPlan.Test;
import edu.buffalo.cse562.physicalPlan.Tuple;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.sql.expression.evaluator.AndVisitor;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import edu.buffalo.cse562.sql.expression.evaluator.ExpressionSplitter;

public class components {

	logManager lg;
	List<Column> tableMap;
	Map<String, ArrayList<String>> tableColTypeMap;
	ArrayList<SelectExpressionItem> projectStmt;
	ArrayList tableJoins;
	Expression whereClause;
	String tableDir;
	String swapDir;
	FromItem fromItem;
	SelectBody selectBody;
	private List orderbyElements;
	private Limit limit;
	StringBuffer planPrint;
	HashMap<String, List<Expression>> singleTableMap = new HashMap<String, List<Expression>>();
	List<Expression> eList = new ArrayList<Expression>();
	ArrayList<Expression> onExpressionList = new ArrayList<Expression>();
	ArrayList<List<String>> onTableLists = new ArrayList<List<String>>();
	ArrayList<Expression> otherList = new ArrayList<Expression>();
	ArrayList<String> joinedTables = new ArrayList<String>();

	public components() {

		tableMap = new ArrayList<Column>();
		tableColTypeMap = new HashMap<String, ArrayList<String>>();
		planPrint = new StringBuffer();
		lg = new logManager();
	}

	public void initializeParam() {
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

	public void addToPlan(String s) {
		planPrint.append(s);
	}

	public void printPlan() {
		System.out.println(planPrint.toString());
	}

	public Operator executePhysicalPlan() {
		Operator oper = null;

		if (whereClause != null) {
			AndVisitor calc = new AndVisitor();
			whereClause.accept(calc);
			//			System.out.println(calc.getList());
			List<Expression> expList = calc.getList();
			for(Expression e:expList){
				ExpressionSplitter split = new ExpressionSplitter();
				e.accept(split);
				//				System.out.println("Number of columns in this expression is "+split.getColumnCounter());
				//				System.out.println(split.getTableList());
				if(split.getColumnCounter()==1){
					eList = singleTableMap.get(split.getTableList().get(0));
					if (eList== null) {
						//						System.out.println();
						eList = new ArrayList<Expression>();
					}
					eList.add(e);
					singleTableMap.put(split.getTableList().get(0), eList);
				} else if (split.getColumnCounter()==2) {
					onExpressionList.add(e);
					onTableLists.add(split.getTableList());
				} else {
					otherList.add(e);
				}
			}
		}
		//		System.out.println(singleTableMap);
		//		System.out.println("TableDir----->"+tableDir);
		FromItemParser fip = new FromItemParser(tableDir, tableMap,
				tableColTypeMap);
		fromItem.accept(fip);
		oper = fip.getOperator();
		//		System.out.println("First table"+fip.getOperatorTableName());
		String operTable = fip.getOperatorTableName();
		eList = singleTableMap.get(operTable);
		Expression leftWhereClause = null;
		if(eList!=null) {
			leftWhereClause = eList.get(0);
			for(int i = 1; i < eList.size(); i++) {
				leftWhereClause = new AndExpression(leftWhereClause, eList.get(i));	
			}

			oper = new SelectionOperator(oper, leftWhereClause);
		}
		joinedTables.add(operTable);


		if (tableJoins != null) {
			TupleStruct.setJoinCondition(true);
			Iterator joinIte = tableJoins.iterator();
			while (joinIte.hasNext()) {
				Join joinTable = (Join) joinIte.next();
				fip = new FromItemParser(tableDir, tableMap, tableColTypeMap);

				joinTable.getRightItem().accept(fip);
				Operator rightOper = fip.getOperator();
				//				System.out.println("NAME"+fip.getOperatorTableName());
				//				System.out.println("Right table "+fip.getOperatorTableName());
				String rightTable = fip.getOperatorTableName();
				eList = singleTableMap.get(rightTable);

				Expression rightWhereClause = null;
				if(eList!=null) {
					rightWhereClause = eList.get(0);
					for(int i = 1; i < eList.size(); i++) {
						rightWhereClause = new AndExpression(rightWhereClause, eList.get(i));
					}
					//					System.out.println(rightWhereClause);
					rightOper = new SelectionOperator(rightOper, rightWhereClause);
				}
//				System.out.println(onExpressionList);
//				System.out.println(onTableLists);
				Expression onExpression = null;
				if(joinTable.getOnExpression()==null){
					joinedTables.add(rightTable);
					for(int i=0;i<onTableLists.size();i++) {
						Boolean onExpFlag = true;
						for(String tableName:onTableLists.get(i)) {
							if(!joinedTables.contains(tableName)){
								onExpFlag = false;
							}
						}
						if(onExpFlag==true){
							onExpression = onExpressionList.get(i);
							onExpressionList.remove(i);
							onTableLists.remove(i);
							break;
						}
					}
				} else {
					onExpression = joinTable.getOnExpression();
				}
				//				System.out.println("Joined tables---"+joinedTables);
				//				System.out.println("join on condition"+onExpression);

				oper = new BNLJoinOperator(oper, rightOper,
						onExpression);
			}
		}

		Expression fullWhereClause = null;
		if(onExpressionList!=null&&onExpressionList.size()!=0) {
			//			System.out.println("Right only selection exists!!!!"+eList);
			fullWhereClause = onExpressionList.get(0);
			for(int i = 1; i < onExpressionList.size(); i++) {
				fullWhereClause = new AndExpression(fullWhereClause, onExpressionList.get(i));
			}
			//			System.out.println(rightWhereClause);
		}
		if(otherList!=null&&otherList.size()!=0) {
				if (fullWhereClause==null){
					fullWhereClause = otherList.get(0);
				} else {
					fullWhereClause = new AndExpression(fullWhereClause, otherList.get(0));
				}
				for(int i = 1; i < otherList.size(); i++) {
					fullWhereClause = new AndExpression(fullWhereClause, otherList.get(i));
				}
			//			System.out.println(rightWhereClause);
		}
		if (fullWhereClause!=null){
			oper = new SelectionOperator(oper, fullWhereClause);
		}

		boolean isFunction = false;
		for (SelectExpressionItem sei : projectStmt) {
			Expression e = sei.getExpression();
			if (e instanceof Function)
				isFunction = true;
		}

		if (((PlainSelect) selectBody).getGroupByColumnReferences() != null) {
			// Groupby computation
			PlainSelect select = (PlainSelect) selectBody;
			List<Column> groupbyList = select.getGroupByColumnReferences();
			oper = new GroupbyOperator(oper, projectStmt,
					groupbyList);
		} else if (isFunction) {
			oper = new AggregateOperator(oper, projectStmt);
		} else {
			//			System.out.println("Entering projection");
			oper = new ProjectionOperator(oper, projectStmt);
		}

		if (orderbyElements != null) {
			//			System.out.println("Entering ExternalSort");
			oper = new ExternalSort(oper, "master", orderbyElements, swapDir);
		}

		if (limit != null) {
			//			System.out.println("Entering Limit");
			oper = new LimitOperator(oper, limit.getRowCount());
		}
		return oper;
	}

	public void OrderBy(ArrayList<Datum[]> list) {
		if (list == null)
			return;
		OrderByOperator obp = new OrderByOperator(orderbyElements);
		obp.setListDatum(list);
		if (orderbyElements != null) {
			obp.sort();
		}
		obp.print();
	}

	public void processTuples(Operator oper) {
		// OrderByOperator obp = new OrderByOperator(orderbyElements);
		Datum[] t = oper.readOneTuple();
		while (t != null) {
			// if (orderbyElements != null) {
			// obp.addTuple(t);
			// } else {
			printTuple(t);
			// }
			t = oper.readOneTuple();
		}
		// if (orderbyElements != null) {
		// obp.sort();
		// obp.print();
		// }
	}

	private void printGroupTuples(ArrayList<Datum[]> finalGroupbyArrayList) {
		//		System.out
		//		.println("------------PRINTING TUPLE FROM GROUPBY OPERATOR--------");
		for (Datum[] singleDatum : finalGroupbyArrayList) {
			printTuple(singleDatum);
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
		//		System.out.println("Setting to "+tableDir);
		this.tableDir = tableDir;

	}

	public void setFromItems(FromItem fromItem) {
		this.fromItem = fromItem;

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

	public void addColsTypeToTable(
			Map<String, ArrayList<String>> tableColTypeMap) {
		this.tableColTypeMap = tableColTypeMap;

	}

	public void addColsToTable(ArrayList<Column> columnNameList) {
		tableMap.addAll(columnNameList);

	}

	public void addJoins(List joins) {
		this.tableJoins = (ArrayList) joins;

	}

	public void addOrderBy(List orderByElements) {
		this.orderbyElements = orderByElements;

	}

	public void setSwapDirectory(String swapDir) {
		this.swapDir = swapDir;

	}

	public void addLimit(Limit limit) {
		this.limit = limit;

	}
	
	public void resetParam() {
		projectStmt = null;
		whereClause = null;
		selectBody = null;
		fromItem = null;
		planPrint = new StringBuffer();
		orderbyElements = null;
		limit = null;
		tableJoins = null;
	}

}
