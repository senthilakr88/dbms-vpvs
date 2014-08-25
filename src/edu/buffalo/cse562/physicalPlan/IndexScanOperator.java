package edu.buffalo.cse562.physicalPlan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import jdbm.PrimaryHashMap;
import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import edu.buffalo.cse562.sql.expression.evaluator.IndexLeftColumnRightValueFetchingVisitor;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.Datum.Row;
import edu.buffalo.cse562.structure.indexlonglistserializable;
import edu.buffalo.cse562.structure.indexlongserializer;
import edu.buffalo.cse562.structure.indexrowserializer;

public class IndexScanOperator implements Operator{
	String indexDir;
	String fileName;
	String indexFileName;
	List<String> indexNameList;
	ArrayList<Expression> conditionList;
	Map<String, ArrayList<Column>> tableMap;
	Map<String, ArrayList<String>> tableColTypeMap;
	RecordManager recMan = null;
	PrimaryStoreMap<Long,Row> primStoreMap;
	PrimaryTreeMap<Row, Long> primTreeMap;
	PrimaryTreeMap<Row, List<Long>> secTreeMap;
	List<Long> primaryKeyList = new ArrayList<Long>();
	ListIterator<Long> primaryKeyListIterator = null;
	Boolean isSecIndexAvailable = false;
	IndexLeftColumnRightValueFetchingVisitor visitor1 = new IndexLeftColumnRightValueFetchingVisitor();
	IndexLeftColumnRightValueFetchingVisitor visitor2 = new IndexLeftColumnRightValueFetchingVisitor();

	public IndexScanOperator(String indexFileName, List<String> indexNameList, String indexDir, String fileName, ArrayList<Expression> conditionList, 
			Map<String, ArrayList<Column>> tableMap, Map<String, ArrayList<String>> tableColTypeMap, RecordManager recMan){
		this.fileName = fileName;
		this.indexDir = indexDir;
		this.indexFileName = indexFileName;
		this.conditionList = conditionList;
		this.indexNameList = indexNameList;
		this.tableMap = tableMap;
		this.tableColTypeMap = tableColTypeMap;
		try {
			if(recMan == null) 
				this.recMan = RecordManagerFactory.createRecordManager(indexDir+File.separator+fileName);
			else
				this.recMan = recMan;
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		System.out.println("indexFileName " + indexFileName);
//		System.out.println("indexNameList " + indexNameList);
//		System.out.println("indexDir " +indexDir);
//		System.out.println("fileName " +fileName);
//		System.out.println("conditionList " + conditionList);
//		System.out.println("tableMap " + tableMap);
//		System.out.println("tableColTypeMap " + tableColTypeMap);
		getTupleFromIndex();
	}

	@Override
	public void resetTupleMapping() {
	}

	@Override
	public void resetStream() {
	}
	
	public List<Long> getPrimaryKeyLongList(){
		return primaryKeyList;
	}
	
	
	
	private void getTupleFromIndex(){
		fileName = fileName.toLowerCase();
		ArrayList<Column> columnNameList = tableMap.get(fileName);
		ArrayList<String> columnTypeList = tableColTypeMap.get(fileName);
		tableColTypeMap = null;
		tableMap = null;
		indexrowserializer indexRowSerialized = new indexrowserializer(columnNameList,columnTypeList);
		primStoreMap = recMan.storeMap(fileName+"map", indexRowSerialized);

//		List<Integer> pkColIndex1 = null;
//		pkColIndex1 = TupleStruct.getColPositions(TupleStruct.getColString(columnNameList), indexNameList);
//		indexrowserializer primaryKeyIndexSer = new indexrowserializer(TupleStruct.getShrList(columnNameList, pkColIndex1),TupleStruct.getShrList(columnTypeList, pkColIndex1));
//		primTreeMap = recMan.treeMap(fileName, new indexlongserializer(), primaryKeyIndexSer);
		
//		if(primTreeMap == null){
//			System.out.println("NULL - PRIM TREE MAP (INSIDE CONSTRUCTOR)");
//		}
//		System.out.println(primTreeMap.isEmpty());

		//		printTuple(primStoreMap.get(147810).getDatum());


		List<Integer> pkColIndex2 = null;
		pkColIndex2 = TupleStruct.getColPositions(TupleStruct.getColString(columnNameList), indexNameList);
		indexrowserializer indexRowSerialized1 = new indexrowserializer(TupleStruct.getShrList(columnNameList, pkColIndex2), TupleStruct.getShrList(columnTypeList, pkColIndex2));
		indexlonglistserializable indexLongListSerialized = new indexlonglistserializable();
		secTreeMap = recMan.treeMap(indexFileName, indexLongListSerialized, indexRowSerialized1);

		if(!conditionList.isEmpty()){
			if(conditionList.size() == 1){
				//				System.out.println("SINGLE CONDITION");
				Expression singleExpression = conditionList.get(0);
				singleExpression.accept(visitor1);
				Datum singleDatumValue = visitor1.getValue();
				Datum[] singleTuple = new Datum[1];
				singleTuple[0] = singleDatumValue;
				Row singleRow = new Row(singleTuple);
				if(fileName.equals(indexFileName)){
//					System.out.println("FILE NAME AND INDEX NAME SAME - NO SECONDARY INDEX - Not handled");
					if(visitor1.getExressionOperator().equals("<")){
						System.out.println("Less than yet to be handled");
					}
					else if(visitor1.getExressionOperator().equals(">=")){
						System.out.println("Greater than equals yet to be handled");
					}
					else if(visitor1.getExressionOperator().equals(">")){
						System.out.println("Greater than yet to be handles");
					}
				}
				else{
					//					System.out.println("-------->FILE NAME AND INDEX NAME NOT SAME - SECONDARY INDEX IS AVAILABLE<--------");
					isSecIndexAvailable = true;
					if(visitor1.getExressionOperator().equals("<=")){
						SortedMap<Row, List<Long>> primaryRowsFromPrimaryIndex2 = secTreeMap.headMap(singleRow);
						List<Long> outputFromIndexInMapTemp = secTreeMap.get(singleRow);
						SortedMap<Row, List<Long>> outputFromSecondaryIndexInMap = new TreeMap<Row,List<Long>>();
						outputFromSecondaryIndexInMap.putAll(primaryRowsFromPrimaryIndex2);
						outputFromSecondaryIndexInMap.put(singleRow, outputFromIndexInMapTemp);
						for (Entry<Row, List<Long>> singleEntry : outputFromSecondaryIndexInMap.entrySet())
						{
							List<Long> singleRowTemp = singleEntry.getValue();
							for(Long itr: singleRowTemp){
								primaryKeyList.add(itr);
							}
						}
						primaryKeyListIterator = primaryKeyList.listIterator();
					}
					else if(visitor1.getExressionOperator().equals("<")){
						SortedMap<Row, List<Long>> primaryRowsFromPrimaryIndex2 = secTreeMap.headMap(singleRow);
						for (Entry<Row, List<Long>> singleEntry : primaryRowsFromPrimaryIndex2.entrySet())
						{
							List<Long> singleRowTemp = singleEntry.getValue();
							for(Long itr: singleRowTemp){
								primaryKeyList.add(itr);
							}
						}
						primaryKeyListIterator = primaryKeyList.listIterator();
					}
					else if(visitor1.getExressionOperator().equals(">=")){
						System.out.println("Greater than equals yet to be handled");
					}
					else if(visitor1.getExressionOperator().equals(">")){
						SortedMap<Row, List<Long>> primaryRowsFromPrimaryIndex2 = secTreeMap.tailMap(singleRow);
						primaryRowsFromPrimaryIndex2.remove(secTreeMap.get(singleRow));
						for (Entry<Row, List<Long>> singleEntry : primaryRowsFromPrimaryIndex2.entrySet())
						{
							List<Long> singleRowTemp = singleEntry.getValue();
							for(Long itr: singleRowTemp){
								primaryKeyList.add(itr);
							}
						}
						primaryKeyListIterator = primaryKeyList.listIterator();	
					}
					else if(visitor1.getExressionOperator().equals("<>")){
						List<Long> tempRow = secTreeMap.get(singleRow);
						Collection<List<Long>> temp = secTreeMap.values();
						temp.remove(tempRow);
						for(List<Long> itr : temp){
							for(Long rowItr : itr){
								primaryKeyList.add(rowItr);
							}
						}
						primaryKeyListIterator = primaryKeyList.listIterator();
					}	
					else if(visitor1.getExressionOperator().equals("=")){
						List<Long> tempRow = secTreeMap.get(singleRow);
						for(Long itr : tempRow){
							primaryKeyList.add(itr);
						}
						primaryKeyListIterator = primaryKeyList.listIterator();
					}	
				}	
			}
			else if(conditionList.size() == 2){
				//				System.out.println("TWO CONDITION");
				Expression singleExpression1 = conditionList.get(0);
				IndexLeftColumnRightValueFetchingVisitor visitor1 = new IndexLeftColumnRightValueFetchingVisitor();
				singleExpression1.accept(visitor1);
				Datum singleDatumValue1 = visitor1.getValue();
				Datum[] singleTuple1 = new Datum[1];
				singleTuple1[0] = singleDatumValue1;
				Row singleRow1 = new Row(singleTuple1);
				//				System.out.println("VISITOR1 OPERATOR: "+ visitor1.getExressionOperator());

				Expression singleExpression2 = conditionList.get(1);
				IndexLeftColumnRightValueFetchingVisitor visitor2 = new IndexLeftColumnRightValueFetchingVisitor();
				singleExpression2.accept(visitor2);
				Datum singleDatumValue2 = visitor2.getValue();
				Datum[] singleTuple2 = new Datum[1];
				singleTuple2[0] = singleDatumValue2;
				Row singleRow2 = new Row(singleTuple2);
				
				if(fileName.equals(indexFileName)){
//					System.out.println("SECONDARY INDEX NOT AVAILABLE");
					if (visitor1.getExressionOperator().equals("<=") && visitor2.getExressionOperator().equals(">=")){
						
						SortedMap<Row, Long> outputFromPrimaryMap1 = null;
						SortedMap<Row, Long> outputFromPrimaryMap2 = new TreeMap<Row,Long>();
						
//						printTuple(singleRow1.getDatum());
//						printTuple(singleRow2.getDatum());
						
						List<Integer> pkColIndex1 = null;
						pkColIndex1 = TupleStruct.getColPositions(TupleStruct.getColString(columnNameList), indexNameList);
						indexrowserializer primaryKeyIndexSer = new indexrowserializer(TupleStruct.getShrList(columnNameList, pkColIndex1),TupleStruct.getShrList(columnTypeList, pkColIndex1));
						primTreeMap = recMan.treeMap(fileName, new indexlongserializer(), primaryKeyIndexSer);
						
						outputFromPrimaryMap1 = primTreeMap.subMap(singleRow2, singleRow1); //from key inclusive tokey exclusive
						outputFromPrimaryMap2.putAll(outputFromPrimaryMap1);
						outputFromPrimaryMap2.put(singleRow1, primTreeMap.get(singleRow1)); //get the upper limit key

						for (Entry<Row, Long> singleEntry : outputFromPrimaryMap2.entrySet())
						{
							primaryKeyList.add(singleEntry.getValue());
//							System.out.println(singleEntry.getValue());
						}
						primaryKeyListIterator = primaryKeyList.listIterator();
//						System.out.println("PrimaryKeyList after adding long: "+ primaryKeyList.size());
					}
				}
				else{
					isSecIndexAvailable = true;
					SortedMap<Row,List<Long>> outputFromPrimaryMap1 = new TreeMap<Row,List<Long>>();
					if(visitor1.getExressionOperator().equals(">=") && visitor2.getExressionOperator().equals("<")){
						SortedMap<Row, List<Long>> outputFromPrimaryMap = null;
						outputFromPrimaryMap = secTreeMap.subMap(singleRow1, singleRow2);
						for (Entry<Row, List<Long>> singleEntry : outputFromPrimaryMap.entrySet())
						{
							List<Long> singleRowTemp = singleEntry.getValue();
							for(Long itr: singleRowTemp){
								primaryKeyList.add(itr);
							}
						}
						primaryKeyListIterator = primaryKeyList.listIterator();
					}
					else if(visitor1.getExressionOperator().equals("<") && visitor2.getExressionOperator().equals(">=")){
						SortedMap<Row, List<Long>> outputFromPrimaryMap = null;
						outputFromPrimaryMap = secTreeMap.subMap(singleRow2, singleRow1);
						for (Entry<Row, List<Long>> singleEntry : outputFromPrimaryMap.entrySet())
						{
							List<Long> singleRowTemp = singleEntry.getValue();
							for(Long itr: singleRowTemp){
								primaryKeyList.add(itr);
							}
						}
						primaryKeyListIterator = primaryKeyList.listIterator();
					}
					else if(visitor1.getExressionOperator().equals(">=") && visitor2.getExressionOperator().equals("<=")){
						System.out.println("GTE - Not handled!!!");
					}
					else if(visitor1.getExressionOperator().equals("<=") && visitor2.getExressionOperator().equals(">=")){
						SortedMap<Row, List<Long>> outputFromPrimaryMap = secTreeMap.subMap(singleRow2, singleRow1);
						List<Long> temp = secTreeMap.get(singleRow1);
						outputFromPrimaryMap1.putAll(outputFromPrimaryMap);
						outputFromPrimaryMap1.put(singleRow1, temp);
						for (Entry<Row, List<Long>> singleEntry : outputFromPrimaryMap1.entrySet())
						{
							List<Long> singleRowTemp = singleEntry.getValue();
//							System.out.println(singleRowTemp);
							if(singleRowTemp!=null && singleRowTemp.size() > 0) {
								for(Long itr: singleRowTemp){
									primaryKeyList.add(itr);
								}
							}
						}
						primaryKeyListIterator = primaryKeyList.listIterator();
					}
					else if(visitor1.getExressionOperator().equals("<>") && visitor2.getExressionOperator().equals("<>")){
						List<Long> tempRow1 = secTreeMap.get(singleRow1);
						List<Long> tempRow2 = secTreeMap.get(singleRow2);
						Collection<List<Long>> temp = secTreeMap.values();
						temp.remove(tempRow1);
						temp.remove(tempRow2);
						for(List<Long> itr : temp){
							for(Long rowItr : itr){
								primaryKeyList.add(rowItr);
							}
						}
						primaryKeyListIterator = primaryKeyList.listIterator();
					}	
				}
			}
		}
		
	}	

	
	public void cleanObjects() {
		visitor1 = null;
		visitor2 = null;
		secTreeMap = null;
		primTreeMap = null;
	}
	
	public void cleanRecMan() {
		try {
			recMan.clearCache();
			recMan.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		recMan = null;
	}
	
	@Override
	public Datum[] readOneTuple() {
//		System.out.println("READ ONE TUPLE");
		Datum[] output = null;
		Long outputLong = null;
		Row outputRow = null;
		
		if(primaryKeyListIterator.hasNext()){
//			System.out.println("LIST ITR NOT NULL");
			outputLong = primaryKeyListIterator.next();
			outputRow = primStoreMap.get(outputLong);
			if(outputRow!=null){
				output = outputRow.getDatum();
			}
		}
		
		if(output==null) {
			cleanObjects();
			cleanRecMan();
		}
		
//		if(!isSecIndexAvailable){
//			System.out.println("SECONDARY INDEX NOT AVAILABLE");
//			if(primaryKeyListIterator.hasNext()){
//				System.out.println("LIST ITR NOT NULL");
//				outputLong = primaryKeyListIterator.next();
//				outputRow = primStoreMap.get(outputLong);
//			}
//		}
//		else{
//			System.out.println("SECONDARY INDEX AVAILABLE");
//			Long singleLongFromList = null;
//			if(primaryKeyListIterator.hasNext()){
//				singleLongFromList = primaryKeyListIterator.next();
//			}
//			outputRow = primStoreMap.get(singleLongFromList);
//		}
		
//		System.out.println("PRINTING AT THE END OF READONE TUPLE: ");
//		printTuple(output);
		return output;
	}

	public void printTuple(Datum[] row) {
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
}