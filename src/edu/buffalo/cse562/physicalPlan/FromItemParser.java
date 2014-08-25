package edu.buffalo.cse562.physicalPlan;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import edu.buffalo.cse562.logicalPlan.components;
import edu.buffalo.cse562.sql.expression.evaluator.FileScanChecker;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromItemParser implements FromItemVisitor {

	Operator oper = null;
	String basePath = null;
	Map<String, ArrayList<Column>> tableMap;
	String tableName;
	String swapDir;
	Map<String, String> treeMap;
	String indexDir;
	Map<String, ArrayList<String>> tableColTypeMap;
	StringBuffer planPrint;
	Map<String, ArrayList<Integer>> tableRemoveCols;
	Map<String, ArrayList<Expression>> indexMapping;
	Map<String, List<String>> indexNameListMap;
	private ArrayList<Expression> onExpressionList;
	private ArrayList<List<String>> onTableLists;
	private ArrayList<String> tblJoinStr;

	public FromItemParser(String basePath,
			Map<String, ArrayList<Column>> tableMap,
			Map<String, ArrayList<String>> tableColTypeMap, String swapDir,
			Map<String, ArrayList<Integer>> tableRemoveCols, String indexDir,
			Map<String, String> treeMap,
			Map<String, ArrayList<Expression>> indexMap,
			Map<String, List<String>> indexNameListMap) {
		this.basePath = basePath;
		this.tableMap = tableMap;
		this.treeMap = treeMap;
		this.tableColTypeMap = tableColTypeMap;
		this.planPrint = new StringBuffer();
		this.swapDir = swapDir;
		this.indexNameListMap = indexNameListMap;
		this.indexDir = indexDir;
		this.tableRemoveCols = tableRemoveCols;
		this.indexMapping = indexMap;

	}

	public void addToPlan(String s) {
		s += planPrint.toString() + " ";
		planPrint = new StringBuffer(s);
	}

	public StringBuffer getPlan() {
		return planPrint;
	}

	@Override
	public void visit(Table table) {
		// System.out.println("Came to table");
		if (table.getAlias() != null) {
			// System.out.println("getting alias");
			tableName = table.getAlias();
		} else {
			tableName = table.getWholeTableName();
		}
//		System.out.println("indexMapping :: " + indexMapping);
		if (!indexMapping.containsKey(tableName)) {

			if (onExpressionList != null && onExpressionList.size() != 0) {
				int i,j;
				String joinExpr;
				Boolean indexTbl = false;
				Boolean nonIndexTbl = false;
				String rightTbl = tableName;
				String leftTbl;
				for(j=0;j<tblJoinStr.size();j++) {
					if(tblJoinStr.get(j).contains(rightTbl)){
						if(j == 0)
							j = j+1;
						else
							j = j-1;
						break;
					}
				}
//				System.out.println("j :: "+ j);
//				System.out.println("tblJoinStr.size() :: "+  tblJoinStr.size());
				if(j>=tblJoinStr.size()) {
					j =0;
				}
				leftTbl = tblJoinStr.get(j);
//				System.out.println("tblJoinStr ::"+tblJoinStr);
//				System.out.println("leftTbl ::"+leftTbl);
				for (i = 0; i < onExpressionList.size(); i++) {
					String expr = onExpressionList.get(i).toString();
					if (expr.contains(rightTbl) && expr.contains(leftTbl))
						break;
				}
				
				if(i >= onExpressionList.size()) {
					for(j=0;j<tblJoinStr.size();j++) {
						boolean isEnd = false;
						leftTbl = tblJoinStr.get(j);
						for (i = 0; i < onExpressionList.size(); i++) {
							String expr = onExpressionList.get(i).toString();
							if (expr.contains(rightTbl) && expr.contains(leftTbl)) {
								isEnd=true;
								break;
							}
								
						}
						if(isEnd)
							break;
					}
				}
				
				FileScanChecker fsc = new FileScanChecker(rightTbl);
//				System.out.println("OnExpression :: "+ onExpressionList.get(i));
				onExpressionList.get(i).accept(fsc);
				indexTbl = fsc.getFlyTblCheck(indexMapping);
				nonIndexTbl = fsc.getIndexTblCheck(indexDir, table.getWholeTableName());
//				System.out.println("tableName in Fromitemparser :: "+tableName);
//				System.out.println("indexTbl in Fromitemparser :: "+indexTbl);
//				System.out.println("nonIndexTbl in Fromitemparser :: "+nonIndexTbl);	
				if (!(indexTbl && nonIndexTbl)) {
					oper = new FileScanOperator(table, basePath, tableMap,
							tableColTypeMap, tableRemoveCols);
					TupleStruct.addInFlyTables(tableName);
					addToPlan("[File Scan on :: " + table + "]");
				}
			} else {
				oper = new FileScanOperator(table, basePath, tableMap,
						tableColTypeMap, tableRemoveCols);
				TupleStruct.addInFlyTables(tableName);
				addToPlan("[File Scan on :: " + table + "]");
			}
		} else {
//			System.out.println(indexNameListMap.get(tableName));
//			System.out.println("tableMap :: "+ tableMap);
//			System.out.println("tableColTypeMap :: "+ tableColTypeMap);
			TupleStruct.addInFlyTables(tableName);
			oper = new IndexScanOperator(treeMap.get(tableName),
					indexNameListMap.get(tableName), indexDir, tableName,
					indexMapping.get(tableName), tableMap, tableColTypeMap, null);
			addToPlan("[Index Scan on :: " + table + " :: "
					+ treeMap.get(tableName) + " :: " + tableName + " :: "
					+ indexMapping.get(tableName) + "]");
		}
		// printPlan();
	}

	@Override
	public void visit(SubSelect subSelect) {
		// System.out.println("Came to subselect");
		addToPlan("subSelect on " + subSelect);
		SelectBody selectStmt = subSelect.getSelectBody();
		if (selectStmt instanceof PlainSelect) {
			PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
			// System.out.println(plainSelect.toString());
			components comp = new components();
			comp.initializeParam();
			comp.addQueryColsToTable(tableMap);
			comp.addQueryColsTypeToTable(tableColTypeMap);
			comp.addQueryRemoveCols(tableRemoveCols);
			comp.setTableDirectory(basePath);
			comp.setIndexDirectory(indexDir);
			comp.setSwapDirectory(swapDir);
			comp.addProjectStmts(plainSelect.getSelectItems());
			comp.setSelectBody(selectStmt);
			comp.setFromItems(plainSelect.getFromItem());
			comp.addWhereConditions(plainSelect.getWhere());
			comp.addOrderBy(plainSelect.getOrderByElements());
			comp.addJoins(plainSelect.getJoins());
			comp.addLimit(plainSelect.getLimit());
			comp.addFileSize(fileSizeComp(plainSelect.getJoins()));

			TupleStruct.setNestedCondition(true);
			// printPlan();
			oper = comp.executePhysicalPlan();

		} else {
			System.out
					.println("This subselect statement is yet to be handled :: "
							+ selectStmt);
		}
	}

	public void printPlan() {
		System.out.println();
		System.out.println("-------------------------------------------------");
		System.out.println(planPrint.toString());
		System.out.println("-------------------------------------------------");
		System.out.println();
	}

	@Override
	public void visit(SubJoin subJoin) {
		addToPlan("subJoin unimplemented on " + subJoin);
	}

	public Operator getOperator() {
		return oper;
	}

	public String getOperatorTableName() {
		return tableName;
	}

	private Long fileSizeComp(List joins) {
		boolean first = true;
		Long maxSize = null;
		String basePath1 = basePath + File.separator;
		if (!(new File(basePath1).exists())) {
			basePath1 = new File("").getAbsolutePath() + File.separator
					+ basePath1;
		}
		if (joins != null) {
			Iterator JoinIte = joins.iterator();
			// System.out.println(joins);
			while (JoinIte.hasNext()) {
				Join j = (Join) JoinIte.next();
				if (j.getRightItem() instanceof Table) {
					Table t = (Table) j.getRightItem();
					String tableName = t.getName();
					File f = new File(basePath1 + File.separator + tableName
							+ ".dat");
					if (first) {
						maxSize = f.length();
						first = false;
					} else if (maxSize.compareTo(f.length()) < 0) {
						maxSize = f.length();
					}
					// System.out.println("From Item Parser");
					// System.out.println(tableName + " :: " + f.length() + "::"
					// + maxSize);
				}
			}
		} else {
			maxSize = Long.valueOf(1000000);
		}
		return maxSize;
	}

	public void setExprList(ArrayList<Expression> onExpressionList) {
		this.onExpressionList = onExpressionList;

	}

	public void setJoins(ArrayList<String> tableJoins) {
		this.tblJoinStr = tableJoins;

	}
}
