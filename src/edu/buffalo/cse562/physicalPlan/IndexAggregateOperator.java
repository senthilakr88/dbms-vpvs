package edu.buffalo.cse562.physicalPlan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.indexlonglistserializable;
import edu.buffalo.cse562.structure.indexlongserializer;
import edu.buffalo.cse562.structure.indexrowserializer;
import edu.buffalo.cse562.structure.Datum.Row;
import edu.buffalo.cse562.structure.Datum.dLong;

public class IndexAggregateOperator implements Operator {

	List<SelectExpressionItem> selectExpressionList;
	String indexDir;
	Table table;
	boolean first;
	RecordManager recMan;
	String tableName;
	PrimaryStoreMap<Long,Row> pkStoreMap;
	ArrayList<Column> cols;
	ArrayList<String> type;
	List<String> metaInfo;
		
	
	public IndexAggregateOperator(ArrayList<SelectExpressionItem> selectExpressionList,
			String indexDir, Table table, Map<String, ArrayList<Column>> masterTableMap, 
			Map<String, ArrayList<String>> masterTableColTypeMap, Map<String, List<String>> metaInfo) {
		this.selectExpressionList = selectExpressionList;
		this.indexDir = indexDir;
		this.table = table;
		this.tableName = table.getName().toLowerCase();
		try {
			recMan = RecordManagerFactory.createRecordManager(indexDir+File.separator+tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.first = true;
		this.cols = masterTableMap.get(tableName);
		this.type = masterTableColTypeMap.get(tableName);
		this.metaInfo = metaInfo.get(tableName);
	}

	@Override
	public void resetStream() {
		// TODO Auto-generated method stub

	}

	@Override
	public Datum[] readOneTuple() {
		if(first) {
			Datum[] datum = new Datum[1];
			Integer size;
			for (int itr = 0; itr < selectExpressionList.size(); itr++) {
				SelectExpressionItem newItem = selectExpressionList.get(itr);
				Expression e = newItem.getExpression();
				if(e instanceof Function) {
					Function aggFn = (Function) e;
					String column = newItem.toString().replaceFirst(aggFn.getName(), "").replace("(", "").replace(")", "").toLowerCase();
//					System.out.println(aggFn.getName());
					if(column.equals("*")) {
						indexrowserializer indexRowSerialized = new indexrowserializer(cols,type);
						pkStoreMap = recMan.storeMap(tableName+"map", indexRowSerialized);
						size = pkStoreMap.size();
//						System.out.println("size is :: "+size);
						datum[0] = new dLong(size.longValue(),null);
					} else {
						String indexName = null;
						
						for(int i=0;i<metaInfo.size();i++) {
							String pkCol = metaInfo.get(i);
							String[] pkColsList = pkCol.split("::");
							if(pkColsList[0].split(",").length > 1)
								continue;
							if(pkColsList[0].trim().equals(column)) {
								indexName = pkColsList[1];
							}
						}
						if(indexName.equals(tableName)) {
							List<String> tempIdxCols = new ArrayList<String>();
							tempIdxCols.add(column);
							List<Integer> pkColIndex = TupleStruct.getColPositions(
									TupleStruct.getColString(cols), tempIdxCols);
							indexrowserializer pkIndexSer = new indexrowserializer(
									TupleStruct.getShrList(cols, pkColIndex),
									TupleStruct.getShrList(type, pkColIndex));
							PrimaryTreeMap<Row, Long> pkTreeMap = recMan.treeMap(indexName,
									new indexlongserializer(), pkIndexSer);
							size = pkTreeMap.size();
							datum[0] = new dLong(size.longValue(),null);
						} else {
							List<String> tempIdxCols = new ArrayList<String>();
							tempIdxCols.add(column);
							List<Integer> secColIndex = TupleStruct.getColPositions(
									TupleStruct.getColString(cols), tempIdxCols);
							indexrowserializer secIndexSer = new indexrowserializer(
									TupleStruct.getShrList(cols, secColIndex),
									TupleStruct.getShrList(type, secColIndex));
							PrimaryTreeMap<Row, List<Long>> secTreeMap = recMan.treeMap(indexName,
									new indexlonglistserializable(), secIndexSer);
							size = secTreeMap.size();
							datum[0] = new dLong(size.longValue(),null);
						}
						
					}
						
				}
			}
			first = false;
			return datum;
		} else {
			return null;
		}
		
	}

	@Override
	public void resetTupleMapping() {
		// TODO Auto-generated method stub

	}

}
