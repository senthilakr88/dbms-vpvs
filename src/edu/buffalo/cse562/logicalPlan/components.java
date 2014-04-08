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
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.AggregateOperator;
import edu.buffalo.cse562.physicalPlan.BNLJoinOperator;
import edu.buffalo.cse562.physicalPlan.Datum;
import edu.buffalo.cse562.physicalPlan.ExternalSort;
import edu.buffalo.cse562.physicalPlan.FromItemParser;
import edu.buffalo.cse562.physicalPlan.GroupbyOperator;
import edu.buffalo.cse562.physicalPlan.HHJoinOperator;
import edu.buffalo.cse562.physicalPlan.LimitOperator;
import edu.buffalo.cse562.physicalPlan.Operator;
import edu.buffalo.cse562.physicalPlan.OrderByOperator;
import edu.buffalo.cse562.physicalPlan.ProjectionOperator;
import edu.buffalo.cse562.physicalPlan.SelectionOperator;
import edu.buffalo.cse562.physicalPlan.SortMergeJoinOperator;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.sql.expression.evaluator.AndVisitor;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import edu.buffalo.cse562.sql.expression.evaluator.ColumnFetcher;
import edu.buffalo.cse562.sql.expression.evaluator.EqualityCheck;
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
	Long minFileSize;
	Long fileThreshold;
	Boolean firstTime;
//	Map<String, String> joinCol;

	public components() {

		tableMap = new ArrayList<Column>();
		tableColTypeMap = new HashMap<String, ArrayList<String>>();
		planPrint = new StringBuffer();
		lg = new logManager();
	}

	public void initializeParam() {
		projectStmt = new ArrayList<SelectExpressionItem>();
		this.fileThreshold = Long.valueOf(50000000);
		this.firstTime = true;
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
		s += "\n" + planPrint.toString();
		planPrint = new StringBuffer(s);
	}

	public void printPlan() {
		System.out.println();
		System.out.println("-------------------------------------------------");
		System.out.println(planPrint.toString());
		System.out.println("-------------------------------------------------");
		System.out.println();
	}

	public Operator executePhysicalPlan() {
		Operator oper = null;
		Boolean singleTableFlag = false;

		if (whereClause != null) {
			List<Expression> expList = new ArrayList<Expression>();
			try {
				AndVisitor calc = new AndVisitor();
				whereClause.accept(calc);
				// System.out.println(calc.getList());
				expList = calc.getList();
			} catch (UnsupportedOperationException e) {
				expList.add(whereClause);
			}
//			System.out.println(expList);
			for (Expression e : expList) {
				ExpressionSplitter split = new ExpressionSplitter();
				e.accept(split);
//				System.out.println("Number of columns in this expression is "+split.getColumnCounter());
//				System.out.println(split.getTableList());
				if(split.getTableList()!=null&&split.getTableList().get(0)!=null){
					if (split.getColumnCounter() == 1) {
						eList = singleTableMap.get(split.getTableList().get(0));
						if (eList == null) {
							// System.out.println();
							eList = new ArrayList<Expression>();
						}
						eList.add(e);
						singleTableMap.put(split.getTableList().get(0), eList);
					} else if (split.getColumnCounter() == 2) {
						onExpressionList.add(e);
						onTableLists.add(split.getTableList());
					} else {
						otherList.add(e);
					}
				} else {
//					System.out.println("ELSE PART");
					singleTableFlag = true;
					eList = singleTableMap.get("__NONE__");
					if (eList == null) {
						// System.out.println();
						eList = new ArrayList<Expression>();
					}
					eList.add(e);
					singleTableMap.put("__NONE__", eList);					
				}
			}
		}
//		 System.out.println(singleTableMap);
		// System.out.println("TableDir----->"+tableDir);
		FromItemParser fip = new FromItemParser(tableDir, tableMap,
				tableColTypeMap, swapDir);
		fromItem.accept(fip);
		oper = fip.getOperator();
		addToPlan(fip.getPlan().toString());
		// System.out.println("First table"+fip.getOperatorTableName());
		String operTable = fip.getOperatorTableName();
		if(singleTableFlag==true){
			eList = singleTableMap.get("__NONE__");
		} else {
			eList = singleTableMap.get(operTable);	
		}
//		System.out.println(eList);
		Expression leftWhereClause = null;
		if (eList != null) {
			leftWhereClause = eList.get(0);
			for (int i = 1; i < eList.size(); i++) {
				leftWhereClause = new AndExpression(leftWhereClause,
						eList.get(i));
			}
//			System.out.println(leftWhereClause);
			oper = new SelectionOperator(oper, leftWhereClause);
			addToPlan("[Selection on :: " + operTable + " Expr :: "
					+ leftWhereClause.toString() + "]");
		}
		joinedTables.add(operTable);
//		printPlan();
		if (tableJoins != null) {
			TupleStruct.setJoinCondition(true);
			Iterator joinIte = tableJoins.iterator();
			Map<String, String> joinCol = new HashMap<String, String>();
			Map<String, Integer> joinCount = new HashMap<String, Integer>();
			while (joinIte.hasNext()) {
				Join joinTable = (Join) joinIte.next();
				fip = new FromItemParser(tableDir, tableMap, tableColTypeMap, swapDir);

				joinTable.getRightItem().accept(fip);
				Operator rightOper = fip.getOperator();
				addToPlan(fip.getPlan().toString());
				// System.out.println("NAME"+fip.getOperatorTableName());
				// System.out.println("Right table "+fip.getOperatorTableName());
				String rightTable = fip.getOperatorTableName();
				eList = singleTableMap.get(rightTable);

				Expression rightWhereClause = null;
				if (eList != null) {
					rightWhereClause = eList.get(0);
					for (int i = 1; i < eList.size(); i++) {
						rightWhereClause = new AndExpression(rightWhereClause,
								eList.get(i));
					}
					// System.out.println(rightWhereClause);
					rightOper = new SelectionOperator(rightOper,
							rightWhereClause);
					addToPlan("[Selection on :: " + rightTable + " Expr :: "
							+ rightWhereClause.toString() + "]");
				}
				// System.out.println(onExpressionList);
				// System.out.println(onTableLists);
				Expression onExpression = null;
				if (joinTable.getOnExpression() == null) {
					joinedTables.add(rightTable);
					for (int i = 0; i < onTableLists.size(); i++) {
						Boolean onExpFlag = true;
						for (String tableName : onTableLists.get(i)) {
							if (!joinedTables.contains(tableName)) {
								onExpFlag = false;
							}
						}
						if (onExpFlag == true) {
							onExpression = onExpressionList.get(i);
							onExpressionList.remove(i);
							onTableLists.remove(i);
							break;
						}
					}
				} else {
					onExpression = joinTable.getOnExpression();
				}
				// System.out.println("Joined tables---"+joinedTables);
				// System.out.println("join on condition"+onExpression);
//				printPlan();
				Boolean equalityCheck;
				if (onExpression != null) {
					EqualityCheck ec = new EqualityCheck();
					try {
						onExpression.accept(ec);
						equalityCheck = true;
					} catch (Exception e) {
						equalityCheck = false;
					}
					if (!equalityCheck) {
						oper = new BNLJoinOperator(oper, rightOper,
								onExpression);
						addToPlan("[Block Nested Join on :: " + joinedTables
								+ " and " + rightTable + " Expr :: "
								+ onExpression.toString() + "]");
					} else {
//						System.out.println(this.minFileSize);
//						System.out.println(this.fileThreshold);
//						System.out.println(this.minFileSize.compareTo(this.fileThreshold));
//						System.out.println(swapDir != null);
//						System.out.println(swapDir.length() > 0);
						if (this.minFileSize.compareTo(this.fileThreshold) > 0 && swapDir != null
								&& swapDir.length() > 0) {
							ArrayList<OrderByElement> obe;
							
							ColumnFetcher cf = new ColumnFetcher(rightTable);
							onExpression.accept(cf);
							OrderByElement temp = new OrderByElement();
							Column lc = cf.getLeftCol();
							Column rc = cf.getRightCol();
							String lcs = lc.getColumnName();
							String rcs = rc.getColumnName();
							Table lt = cf.getLeftTab();
							Table rt = cf.getRightTab();
							String lts = lt.getAlias() == null ? lt.getName() : lt.getAlias();
							String rts = rt.getAlias() == null ? rt.getName() : rt.getAlias();
							if (!joinCol.containsKey(lts) || !lcs
									.equalsIgnoreCase(joinCol
											.get(lts))) {
								joinCol.put(lts, lcs);
								if(!joinCount.containsKey(lts)) {
									joinCount.put(lts, 1);
								} else {
									int tempCount = joinCount.get(lts);
									joinCount.put(lts, ++tempCount);
								}
								
								temp.setExpression(lc);
								obe = new ArrayList<OrderByElement>();
								obe.add(temp);
								addToPlan("[External Sort on :: "
										+ lts + " OrderBy :: "
										+ obe.toString() + "]");
								oper = new ExternalSort(oper, lts, obe,
										swapDir, joinCount.get(lts));
								firstTime = false;
							}
							if (!joinCol.containsKey(rts)
									|| !rcs.equalsIgnoreCase(joinCol
											.get(rts))) {
								joinCol.put(rts, rcs);
								if(!joinCount.containsKey(rts)) {
									joinCount.put(rts, 1);
								} else {
									int tempCount = joinCount.get(rts);
									joinCount.put(rts, ++tempCount);
								}
								temp = new OrderByElement();
								temp.setExpression(rc);
								obe = new ArrayList<OrderByElement>();
								obe.add(temp);
								addToPlan("[External Sort on :: "
										+ rts + " OrderBy :: "
										+ obe.toString() + "]");
								rightOper = new ExternalSort(rightOper, rts, obe,
										swapDir,joinCount.get(rts));
							}
							addToPlan("[Sort Merge Join on :: " + joinedTables
									+ " and " + rightTable + " Expr :: "
									+ onExpression.toString() + "]");
							oper = new SortMergeJoinOperator(oper, rightOper,
									onExpression);
						} else {
							addToPlan("[Hybrid Hash Join on :: " + joinedTables
									+ " and " + rightTable + " Expr :: "
									+ onExpression.toString() + "]");
							oper = new HHJoinOperator(oper, rightOper,
									onExpression, rightTable);
						}
					}

				} else {
					oper = new BNLJoinOperator(oper, rightOper, onExpression);
					addToPlan("[Block Nested Join on :: " + joinedTables
							+ " and " + rightTable + " No Expression]");
				}
			}
		}
//		printPlan();
		Expression fullWhereClause = null;
		if (onExpressionList != null && onExpressionList.size() != 0) {
			// System.out.println("Right only selection exists!!!!"+eList);
			fullWhereClause = onExpressionList.get(0);
			for (int i = 1; i < onExpressionList.size(); i++) {
				fullWhereClause = new AndExpression(fullWhereClause,
						onExpressionList.get(i));
			}
			// System.out.println(rightWhereClause);
		}
		if (otherList != null && otherList.size() != 0) {
			if (fullWhereClause == null) {
				fullWhereClause = otherList.get(0);
			} else {
				fullWhereClause = new AndExpression(fullWhereClause,
						otherList.get(0));
			}
			for (int i = 1; i < otherList.size(); i++) {
				fullWhereClause = new AndExpression(fullWhereClause,
						otherList.get(i));
			}
			// System.out.println(rightWhereClause);
		}
		if (fullWhereClause != null) {
			oper = new SelectionOperator(oper, fullWhereClause);
			addToPlan("[Selection on :: " + joinedTables + " Expr :: "
					+ fullWhereClause.toString() + "]");
		}

		boolean isFunction = false;
		for (SelectExpressionItem sei : projectStmt) {
			Expression e = sei.getExpression();
			if (e instanceof Function)
				isFunction = true;
		}
//		printPlan();
		if (((PlainSelect) selectBody).getGroupByColumnReferences() != null) {
			// Groupby computation
			PlainSelect select = (PlainSelect) selectBody;
			List<Column> groupbyList = select.getGroupByColumnReferences();
			oper = new GroupbyOperator(oper, projectStmt, groupbyList);
			addToPlan("[Group By on :: " + joinedTables + " Groupby :: "
					+ groupbyList.toString() + "]");
			addToPlan("[Projection on :: " + joinedTables + " Columns :: "
					+ projectStmt.toString() + "]");
		} else if (isFunction) {
			oper = new AggregateOperator(oper, projectStmt);
			addToPlan("[Aggregate on :: " + joinedTables + "]");
			addToPlan("[Projection on :: " + joinedTables + " Columns :: "
					+ projectStmt.toString() + "]");
		} else {
			// System.out.println("Entering projection");
			oper = new ProjectionOperator(oper, projectStmt);
			addToPlan("[Projection on :: " + joinedTables + " Columns :: "
					+ projectStmt.toString() + "]");
		}
//		printPlan();
		if (orderbyElements != null) {
			// System.out.println("Entering ExternalSort");
			if (this.minFileSize.compareTo(this.fileThreshold) > 0 && swapDir != null && swapDir.length() > 0) {
				addToPlan("[External Sort on :: " + joinedTables
						+ " OrderBy :: " + orderbyElements.toString() + "]");
				oper = new ExternalSort(oper, "masterExternal",
						orderbyElements, swapDir,1);
			} else {
				List<Datum[]> listDatum = new ArrayList<Datum[]>();
				Datum[] t = oper.readOneTuple();
				while (t != null) {
					listDatum.add(t);
					t = oper.readOneTuple();
				}
				oper = new OrderByOperator(orderbyElements, listDatum);
				addToPlan("[Normal Sort on :: " + joinedTables + " OrderBy :: "
						+ orderbyElements.toString() + "]");
			}

		}
//		printPlan();
		if (limit != null) {
			// System.out.println("Entering Limit");
			oper = new LimitOperator(oper, limit.getRowCount());
			addToPlan("[Limit on :: " + joinedTables + " Rows :: "
					+ limit.getRowCount() + "]");
		}
//		printPlan();
		// oper.resetTupleMapping();
		return oper;
	}

	// public void OrderBy(ArrayList<Datum[]> list) {
	// if (list == null)
	// return;
	// OrderByOperator obp = new OrderByOperator(orderbyElements, list);
	// obp.setListDatum(list);
	// if (orderbyElements != null) {
	// obp.sort();
	// }
	// obp.print();
	// }

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
		// System.out
		// .println("------------PRINTING TUPLE FROM GROUPBY OPERATOR--------");
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
		// System.out.println("Setting to "+tableDir);
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
		firstTime = true;
	}

	public void addFileSize(Long fileSizeComp) {
//				System.out.println(fileSizeComp);
		//		11632
		this.minFileSize = fileSizeComp;

	}

}
