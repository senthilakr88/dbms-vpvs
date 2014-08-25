package edu.buffalo.cse562.logicalPlan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import edu.buffalo.cse562.physicalPlan.IndexScanOperator;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.sql.expression.evaluator.AndVisitor;
import edu.buffalo.cse562.sql.expression.evaluator.ExpressionSplitter;
import edu.buffalo.cse562.sql.expression.evaluator.columnfinder;
import edu.buffalo.cse562.sql.expression.evaluator.insertitemvisitor;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.Datum.dDate;
import edu.buffalo.cse562.structure.Datum.dLong;
import edu.buffalo.cse562.structure.Datum.dString;
import edu.buffalo.cse562.structure.indexlonglistserializable;
import edu.buffalo.cse562.structure.indexlongserializer;
import edu.buffalo.cse562.structure.indexrowserializer;
import edu.buffalo.cse562.structure.Datum.Row;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

public class dmlworker {

	Map<String, List<Statement>> stmtMap;
	Map<String, ArrayList<Column>> tabCols;
	Map<String, ArrayList<String>> tabColsType;
	String indexDir;
	RecordManager recMan;
	PrimaryStoreMap<Long, Row> pkStoreMap;
	PrimaryTreeMap<Row, Long> pkTreeMap;
	List<Integer> pkColIndex;
	Map<List<String>, PrimaryTreeMap<Row, List<Long>>> secTreeMaps;
	ArrayList<Column> cols;
	ArrayList<String> type;
	Map<String, List<String>> metaInfo;

	public dmlworker(String indexDir, Map<String, List<Statement>> stmtMap,
			Map<String, List<String>> metaInfo) {
		this.indexDir = indexDir;
		this.stmtMap = stmtMap;
		this.metaInfo = metaInfo;
	}

	public void processor() {
		for (String key : stmtMap.keySet()) {
			// System.out.println("key :: "+ key);
			// System.out.println("value :: "+ stmtMap.get(key).size());
			useStoreMap(key, metaInfo.get(key).get(0));
			useTreeMap(key, metaInfo.get(key));
			processStmt(key, stmtMap.get(key));
			clearIndexInstance();
		}
	}

	private void useStoreMap(String tblName, String pkCol) {

		try {
			cols = tabCols.get(tblName);
			type = tabColsType.get(tblName);
			indexrowserializer datumIndexSer = new indexrowserializer(cols,
					type);
			indexlongserializer pkLongSer = new indexlongserializer();
			String[] pkColsList = pkCol.split("::");
			String[] pkCols = pkColsList[0].split(",");
			for (int i = 0; i < pkCols.length; i++)
				pkCols[i] = pkCols[i].trim();
			List<String> idxCols = Arrays.asList(pkCols);
			// System.out.println(idxCols);
			pkColIndex = TupleStruct.getColPositions(
					TupleStruct.getColString(cols), idxCols);
			// System.out.println(pkColIndex);
			indexrowserializer pkIndexSer = new indexrowserializer(
					TupleStruct.getShrList(cols, pkColIndex),
					TupleStruct.getShrList(type, pkColIndex));

			recMan = RecordManagerFactory.createRecordManager(indexDir
					+ File.separator + tblName);
			pkStoreMap = recMan.storeMap(tblName + "map", datumIndexSer);
			pkTreeMap = recMan.treeMap(tblName, pkLongSer, pkIndexSer);
			secTreeMaps = new HashMap<List<String>, PrimaryTreeMap<Row, List<Long>>>();
			// System.out.println(tblName);
			// System.out.println(pkTreeMap);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void useTreeMap(String tblName, List<String> secColList) {
		PrimaryTreeMap<Row, List<Long>> secTempTreeMap = null;
		Iterator<String> secColsIte = secColList.iterator();
		secColsIte.next();
		while (secColsIte.hasNext()) {
			String[] secColsAll = secColsIte.next().split("::");
			String[] secCols = secColsAll[0].split(",");
			for (int i = 0; i < secCols.length; i++)
				secCols[i] = secCols[i].trim();
			String idxName = secColsAll[1];
			List<String> idxCols = Arrays.asList(secCols);
			List<Integer> secColIndexTemp = TupleStruct.getColPositions(
					TupleStruct.getColString(cols), idxCols);

			indexrowserializer secIndexSer = new indexrowserializer(
					TupleStruct.getShrList(cols, secColIndexTemp),
					TupleStruct.getShrList(type, secColIndexTemp));

			indexlonglistserializable pkLongSerList = new indexlonglistserializable();
			secTempTreeMap = recMan
					.treeMap(idxName, pkLongSerList, secIndexSer);
			secTreeMaps.put(idxCols, secTempTreeMap);
		}
		// System.out.println(secTreeMaps.keySet());
	}

	private void processStmt(String tblName, List<Statement> list) {
		Iterator<Statement> ite = list.iterator();
		while (ite.hasNext()) {
			Statement stmt = ite.next();
			if (stmt instanceof Insert) {
				insertTuple(tblName, stmt);
			} else if (stmt instanceof Update) {
				updateTuple(tblName, stmt);
			} else if (stmt instanceof Delete) {
				deleteTuple(tblName, stmt);
			}
		}
	}

	private void insertTuple(String tblName, Statement stmt) {
		PrimaryTreeMap<Row, List<Long>> secTempTreeMap = null;
		List<String> idxCols;
		List<Integer> secColIndex;
		// System.out.println(((Insert) stmt).getItemsList());
		insertitemvisitor iiv = new insertitemvisitor(
				((Insert) stmt).getColumns(), cols, type);
		((Insert) stmt).getItemsList().accept(iiv);
		Datum[] datum = iiv.getDatum();
		// printTuple(datum);
		// System.out.println(type);
		Datum[] key = TupleStruct.getDatum(datum, pkColIndex);
		// printTuple(key);
		long s = pkStoreMap.putValue(new Row(datum));
		pkTreeMap.put(new Row(key), s);
		// System.out.println(pkTreeMap.values());
		// System.out.println("size of map :: " + pkTreeMap.size());
		Iterator it = secTreeMaps.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			idxCols = (List<String>) pairs.getKey();
			secTempTreeMap = (PrimaryTreeMap<Row, List<Long>>) pairs.getValue();
			secColIndex = TupleStruct.getColPositions(
					TupleStruct.getColString(cols), idxCols);
			Datum[] secKey = TupleStruct.getDatum(datum, secColIndex);
			Row seckeyob = new Row(secKey);
			if (secTempTreeMap.containsKey(seckeyob)) {
				// System.out.println("Key Already present");
				// printTuple(secKey);
				// printTuple(key);
				List<Long> tempRows = secTempTreeMap.get(seckeyob);
				tempRows.add(s);
				secTempTreeMap.put(seckeyob, tempRows);
			} else {
				// System.out.println("New Key getting created");
				// printTuple(secKey);
				// printTuple(key);
				List<Long> tempRows = new ArrayList<Long>();
				tempRows.add(s);
				secTempTreeMap.put(seckeyob, tempRows);
			}
			// System.out.println("Next Iteration");
		}
	}

	private void updateTuple(String tblName, Statement stmt) {

		// shipdate=date('1997-05-14')
		// RETURNFLAG='R'
		// ORDERPRIORITY='1-URGENT'

		// String value;
		// String columnName;
		// String tableName;
		// String schemaName;
		// String aliasName;

		// <---------USE CASES!!!---->
		// WHERE ORDERDATE >= date('1992-01-01') AND ORDERDATE <
		// date('1993-01-01');
		// WHERE LINENUMBER<2;
		// WHERE shipdate>=date('1997-05-02') AND shipdate<date('1997-05-14');

		// Orderdate - ORDERS (Secondary Index) - Index scan over the ORDERS
		// TABLE, Orderdate index(secTreeIndex)
		// Linenumber - LINEITEM (No Index)
		// Shipdate - LINEITEM (Secondary Index) - Index scan over the ORDERS
		// TABLE, Orderdate index(secTreeIndex)
		// <-------------------------->

//		System.out.println("UPDATE: Where expression: "
//				+ ((Update) stmt).getWhere());
//		System.out.println("UPDATE: Column name: "
//				+ ((Update) stmt).getColumns());
//		System.out.println("UPDATE: Table name: " + ((Update) stmt).getTable());
//		System.out.println("UPDATE: Update value: "
//				+ ((Update) stmt).getExpressions());
		ArrayList<Expression> conditionList = new ArrayList<Expression>();
		int updDatumIndexNumber = -1;
		List<Expression> updateValue = ((Update) stmt).getExpressions();
		Datum newDatum = null;
		Column newColumn = null;

		// 1. construct a new datum from the update statement
		if (!updateValue.isEmpty()) {
			if (updateValue.size() == 1) {
				Expression e1 = updateValue.get(0);
				if (e1 instanceof Function) {
//					System.out.println("Function Expression");
					Function f1 = (Function) e1;
//					System.out.println("Func: " + f1);
					ExpressionList expList = f1.getParameters();
					if (f1.getName().equals("date")) {
//						System.out.println("function name is date");
						List<Expression> expList1 = expList.getExpressions();
						if (expList1 != null && !expList1.isEmpty()) {
							Expression e2 = expList1.get(0);
//							System.out.println("Func param: " + e2);
							String str = ((StringValue) e2).toString();
//							System.out.println("date value in string: " + str);
							newColumn = (Column) ((Update) stmt)
									.getColumns().get(0);
							updDatumIndexNumber = TupleStruct.getColIndex(cols,
									newColumn);
							newDatum = new dDate(((StringValue) e2).getValue(),
									newColumn);
						}
					}
				} else if (e1 instanceof StringValue) {
//					System.out.println("String Expression" + e1);
					newColumn = (Column) ((Update) stmt).getColumns()
							.get(0);
					updDatumIndexNumber = TupleStruct.getColIndex(cols,
							newColumn);
					newDatum = new dString(((StringValue) e1).getValue(),
							newColumn);
//					printTuple(newDatum);
				}
			}
		}

		// 2. Retrieve the List<long> and long from the primary treeMap and
		// secondary treeMap with applying the "where" condition!
		Expression conditionExp = ((Update) stmt).getWhere();
		if (conditionExp instanceof AndExpression) {
			// System.out.println("AND EXP");
			AndExpression andExp = (AndExpression) conditionExp;
			Expression leftExp = andExp.getLeftExpression();
			Expression rightExp = andExp.getRightExpression();
			conditionList.add(leftExp);
			conditionList.add(rightExp);
		} else if (conditionExp instanceof BinaryExpression) {
//			System.out.println("Binary Exp");
			conditionList.add(conditionExp);
		}

//		System.out.println(conditionList);

		// Get the index column names
		ArrayList<String> column = new ArrayList<String>();
		for (int i = 0; i < conditionList.size(); i++) {
			Expression expr = conditionList.get(i);
			columnfinder cf = new columnfinder();
			expr.accept(cf);
			String columnstr = cf.getColumn().toLowerCase();
			if (!column.contains(columnstr)) {
				column.add(columnstr);
			}
		}

//		System.out.println("column :: " + column);
		// Get the index Name
		String indexName = null;
		if (column.size() == 1) {
			List<String> metaSpeInfo = metaInfo.get(tblName);
			for (int i = 0; i < metaSpeInfo.size(); i++) {
				String pkCol = metaSpeInfo.get(i);
				String[] pkColsList = pkCol.split("::");
				if (pkColsList[0].trim().equals(column.get(0))) {
					indexName = pkColsList[1];
				}
			}
		} else {
			System.out
					.println("Still not implement :: dmlworker double column");
		}

		//
		IndexScanOperator indexScanOper = new IndexScanOperator(indexName,
				column, indexDir, tblName, conditionList, tabCols, tabColsType,
				recMan);
		indexScanOper.cleanObjects();
		indexScanOper.cleanRecMan();
		useStoreMap(tblName, metaInfo.get(tblName).get(0));
		useTreeMap(tblName,metaInfo.get(tblName));
		// 3. Retrieve the tuples from the store map with the long values
		// retrieved from the maps in step1
		List<Long> primaryKeyLongList = indexScanOper.getPrimaryKeyLongList();
//		System.out.println("primaryKeyLongList :: " + primaryKeyLongList);
//		System.out.println("updDatumIndexNumber :: " + updDatumIndexNumber);
		// 4. apply the change in the Datum[] and put back the row for the same
		// long value in the storeMap
		
		PrimaryTreeMap<Row, List<Long>> secTempTreeMap = null;
		List<String> idxCols = null;
//		System.out.println("newColumn.getColumnName() :: "+ newColumn.getColumnName());
//		System.out.println(secTreeMaps.keySet());
		Iterator it = secTreeMaps.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			idxCols = (List<String>) pairs.getKey();
			if(idxCols.size() > 1)
				continue;
			if(idxCols.get(0).contains(newColumn.getColumnName().toLowerCase())) {
				secTempTreeMap = (PrimaryTreeMap<Row, List<Long>>) pairs
						.getValue();
				break;
			}
		}
		List<Integer> secTempColIndex = TupleStruct.getColPositions(
				TupleStruct.getColString(cols), idxCols);
		
		if (newDatum != null) {
			for (Long itr : primaryKeyLongList) {
				
				Row singleTupleRow = pkStoreMap.get(itr);
				Datum[] singleTuple = singleTupleRow.getDatum();
				
				//remove from old secondary key
				if(secTempTreeMap!=null) {
					Datum[] secKeyOld = new Datum[secTempColIndex.size()];
					for(int j=0;j<secTempColIndex.size();j++) {
						secKeyOld[j] = singleTuple[secTempColIndex.get(j)];
					}
					Row seckeyobOld=new Row(secKeyOld);
//					System.out.println("old :: "+seckeyobOld);
					List<Long> tempRowsOld = secTempTreeMap
						.get(seckeyobOld);
					tempRowsOld.remove(itr);
					secTempTreeMap.put(seckeyobOld, tempRowsOld);
				}
				//update in pkstore map
				singleTuple[updDatumIndexNumber] = newDatum;
				Row singleTupleRowNew = new Row(singleTuple);
				pkStoreMap.put(itr, singleTupleRowNew);
			
			
				//insert into new secondary key
				if(secTempTreeMap!=null) {
					Datum[] secKeyNew = new Datum[secTempColIndex.size()];
					for(int j=0;j<secTempColIndex.size();j++) {
						secKeyNew[j] = singleTuple[secTempColIndex.get(j)];
					}
					Row seckeyobNew=new Row(secKeyNew);
//					System.out.println("New :: "+ seckeyobNew);
					List<Long> tempRowsNew = secTempTreeMap
						.get(seckeyobNew);
					tempRowsNew.add(itr);
					secTempTreeMap.put(seckeyobNew, tempRowsNew);	 
				}	
					
		
			}
		}
	}


	private void deleteTuple(String tblName, Statement stmt) {
//		System.out.println("stmt ::" + stmt);
		ArrayList<Expression> expList = new ArrayList<Expression>();
		Expression whereCondition = ((Delete) stmt).getWhere();
		try {
			AndVisitor calc = new AndVisitor();
			whereCondition.accept(calc);
			// System.out.println(calc.getList());
			expList = (ArrayList<Expression>) calc.getList();
		} catch (UnsupportedOperationException e) {
			expList.add(whereCondition);
		}

//		System.out.println(pkStoreMap.size());
//		System.out.println(expList);
		// Get the index column names
		ArrayList<String> column = new ArrayList<String>();
		for (int i = 0; i < expList.size(); i++) {
			Expression expr = expList.get(i);
			columnfinder cf = new columnfinder();
			expr.accept(cf);
			String columnstr = cf.getColumn().toLowerCase();
			if (!column.contains(columnstr)) {
				column.add(columnstr);
			}
		}

//		System.out.println("column :: " + column);
		// Get the index Name
		String indexName = null;
		if (column.size() == 1) {
			List<String> metaSpeInfo = metaInfo.get(tblName);
			for (int i = 0; i < metaSpeInfo.size(); i++) {
				String pkCol = metaSpeInfo.get(i);
				String[] pkColsList = pkCol.split("::");
				if (pkColsList[0].trim().equals(column.get(0))) {
					indexName = pkColsList[1];
				}
			}
		} else {
			System.out
					.println("Still not implement :: dmlworker double column");
		}

		//
		IndexScanOperator indexScanOper = new IndexScanOperator(indexName,
				column, indexDir, tblName, expList, tabCols, tabColsType,
				recMan);
		indexScanOper.cleanObjects();
		indexScanOper.cleanRecMan();
		useStoreMap(tblName, metaInfo.get(tblName).get(0));
		useTreeMap(tblName,metaInfo.get(tblName));
//		System.out.println("size of pkStoreMap before delete :: " +pkStoreMap.size());
		List<Long> primaryKeyLongList = indexScanOper.getPrimaryKeyLongList();
//		System.out.println(primaryKeyLongList);
//		System.out.println(pkStoreMap);
		for (Long itr : primaryKeyLongList) {
//			System.out.println(itr);
//			System.out.println(pkStoreMap.get(itr));
			if(itr == null) {
				break;
			}
			Datum[] datum =  pkStoreMap.get(itr).getDatum();
			Datum[] pkKey = new Datum[pkColIndex.size()];
			for(int i=0;i<pkColIndex.size();i++) {
				pkKey[i] = datum[pkColIndex.get(i)];
			}
			pkTreeMap.remove(new Row(pkKey));
			Iterator it = secTreeMaps.entrySet().iterator();
			while(it.hasNext()) {
				Map.Entry pairs = (Map.Entry) it.next();
				List<String> idxCols = (List<String>) pairs.getKey();
				PrimaryTreeMap<Row, List<Long>> secTempTreeMap = (PrimaryTreeMap<Row, List<Long>>) pairs
						.getValue();
				List<Integer> secTempColIndex = TupleStruct.getColPositions(
						TupleStruct.getColString(cols), idxCols);
				Datum[] secKey = new Datum[secTempColIndex.size()];
				for(int j=0;j<secTempColIndex.size();j++) {
					secKey[j] = datum[secTempColIndex.get(j)];
				}
				Row seckeyob=new Row(secKey);
				List<Long> tempRows = secTempTreeMap
						.get(seckeyob);
				tempRows.remove(itr);
				secTempTreeMap.put(seckeyob, tempRows);
			}
			pkStoreMap.remove(itr);
		}
		
		
		
//		 System.out.println("size of pkStoreMap after delete :: " +pkStoreMap.size());
	}

	private void clearIndexInstance() {
		try {
			// System.out.println("commiting");
			recMan.commit();
//			 System.out.println("size of pkStoreMap after commit :: " + pkStoreMap.size());
			recMan.clearCache();
			recMan.close();
			recMan = null;
			pkStoreMap = null;
			pkColIndex = null;
			pkTreeMap = null;
			secTreeMaps = null;
			cols = null;
			type = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void setTabCols(Map<String, ArrayList<Column>> masterTableMap) {
		tabCols = masterTableMap;

	}

	public void setTabColsType(
			Map<String, ArrayList<String>> masterTableColTypeMap) {
		tabColsType = masterTableColTypeMap;
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

	private void printTuple(Datum row) {
		Boolean first = true;
		if (row != null) {
			System.out.println(row.getColumn() + " :: " + row);
		}
	}

}
