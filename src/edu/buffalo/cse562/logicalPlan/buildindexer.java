package edu.buffalo.cse562.logicalPlan;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import jdbm.InverseHashView;
import jdbm.PrimaryHashMap;
import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordListener;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryHashMap;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;
import jdbm.Serializer;
import edu.buffalo.cse562.physicalPlan.FileScanOperator;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.indexlonglistserializable;
import edu.buffalo.cse562.structure.indexlongserializer;
import edu.buffalo.cse562.structure.indexrowlistserializer;
import edu.buffalo.cse562.structure.indexrowserializer;
import edu.buffalo.cse562.structure.Datum.Row;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class buildindexer {

	String tableName;
	Table table;
	ArrayList<Column> columnNameList;
	ArrayList<String> columnTypeList;
	String tableDir;
	String indexDir;
	String preCompDir;
	String tablefile;
	BufferedWriter metaWriter;
	List<Index> indexes;
	List<Index> indexesAddl;

	public buildindexer(String tableDir, String indexDir, String preCompDir) {
		this.tableDir = tableDir;
		this.indexDir = indexDir;
		this.preCompDir = preCompDir;
	}

	private List<Index> additionalIndexes() {
		String indexStr;
		String[] tempIndexArr;
		List<String> indexList = indexTobeCreated();
		List<Index> indexAddl = new ArrayList<Index>();
		Iterator<String> indexIte = indexList.iterator();
		while (indexIte.hasNext()) {
			indexStr = indexIte.next();
			tempIndexArr = indexStr.split("_");
			if (tempIndexArr.length == 2
					&& tempIndexArr[0].equalsIgnoreCase(tableName)) {
				Index tempIndex = new Index();
				List<String> tempCol = new ArrayList<String>();
				tempCol.add(tempIndexArr[1]);
				tempIndex.setColumnsNames(tempCol);
				tempIndex.setName(tempIndexArr[1]);
				tempIndex.setType("INDEX");
				indexAddl.add(tempIndex);
			}
		}
		return indexAddl;
	}

	private List<String> indexTobeCreated() {
		List<String> indexList = new ArrayList<String>();
		indexList.add("lineitem_linenumber");
		indexList.add("lineitem_returnflag");
		indexList.add("orders_orderpriority");
		return indexList;
	}

	public void parseStatement(CreateTable statement) {
		columnNameList = new ArrayList<Column>();
		columnTypeList = new ArrayList<String>();
		List<ColumnDefinition> columnDefinitionList = (ArrayList<ColumnDefinition>) statement
				.getColumnDefinitions();
		table = statement.getTable();
		tableName = statement.getTable().getName().toLowerCase();
		for (ColumnDefinition s : columnDefinitionList) {
			columnNameList.add(new Column(table, s.getColumnName()));
			columnTypeList.add(s.getColDataType().toString());
		}
		String basePath = tableDir + File.separator + tableName + ".dat";
		if (!(new File(basePath).exists())) {
			tablefile = new File("").getAbsolutePath() + File.separator
					+ basePath;
		} else {
			tablefile = basePath;
		}

		indexes = statement.getIndexes();
		indexesAddl = additionalIndexes();
		indexes.addAll(indexesAddl);
		try {
			metaWriter = new BufferedWriter(new FileWriter(indexDir
					+ File.separator + tableName + ".metadata"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long start = System.currentTimeMillis();
		buildIndex();
		System.out.println(tableName + " :: "
				+ (System.currentTimeMillis() - start));
	}

	public void buildIndex() {
		Index idx;
		List<String> idxCols;
		String idxType;
		RecordManager recman;
		String idxName;
		PrimaryStoreMap<Long, Row> pkStoreMap = null;
		PrimaryTreeMap<Row, Long> pkTreeMap = null;
		PrimaryTreeMap<Row, List<Long>> secTempTreeMap = null;
		Map<List<String>, PrimaryTreeMap<Row, List<Long>>> secTreeMaps = null;
		List<Integer> pkColIndex = null;
		List<Integer> secColIndex = null;
		indexrowserializer datumIndexSer = null;
		indexrowserializer pkIndexSer = null;
		indexlongserializer pkLongSer = null;
		indexlonglistserializable pkLongSerList = null;
		indexrowserializer secIndexSer = null;
		
		FileScanOperator input;
		try {

			recman = RecordManagerFactory.createRecordManager(indexDir
					+ File.separator + tableName);
			Iterator<Index> ite = indexes.iterator();
			while (ite.hasNext()) {
				idx = ite.next();
				idxCols = idx.getColumnsNames();
				idxType = idx.getType();
				idxName = idx.getName();
				// System.out.println(tableName + " :: " + idxCols + " :: " +
				// idxName + " :: " + idxType);
				if (idxType.contains("PRIMARY")) {

					pkColIndex = TupleStruct.getColPositions(
							TupleStruct.getColString(columnNameList), idxCols);
					
					pkIndexSer = new indexrowserializer(TupleStruct.getShrList(
							columnNameList, pkColIndex), TupleStruct
							.getShrList(columnTypeList, pkColIndex));

					datumIndexSer = new indexrowserializer(columnNameList,
									columnTypeList);

					pkLongSer = new indexlongserializer();
					
					pkStoreMap = recman.storeMap(tableName+"map",datumIndexSer);
					pkTreeMap = recman.treeMap(
							tableName, pkLongSer, pkIndexSer);
					secTreeMaps = new HashMap<List<String>, PrimaryTreeMap<Row, List<Long>>>();
					metaWriter.append(idxCols.toString().replace("[", "")
							.replace("]", "").trim()
							+ "::" + tableName);
					metaWriter.newLine();
				} else {
					
					secColIndex = TupleStruct.getColPositions(
							TupleStruct.getColString(columnNameList), idxCols);
					
					secIndexSer = new indexrowserializer(TupleStruct.getShrList(
							columnNameList, secColIndex), TupleStruct
							.getShrList(columnTypeList, secColIndex));
					
					pkLongSerList = new indexlonglistserializable();
					
					secTempTreeMap = recman.treeMap(idxName,pkLongSerList,secIndexSer);
					secTreeMaps.put(idxCols, secTempTreeMap);
					metaWriter.append(idxCols.toString().replace("[", "")
							.replace("]", "").trim()
							+ "::" + idxName);
					metaWriter.newLine();
				}
			}
			input = new FileScanOperator(table, tableDir, columnNameList,
					columnTypeList, null);
			// System.out.println(secTreeMaps.keySet());
			int i = 0;
			Datum[] datum = input.readOneTuple();
			while (datum != null) {
				Datum[] key = TupleStruct.getDatum(datum, pkColIndex);
				// printTuple(key);
				long s=pkStoreMap.putValue(new Row(datum));
				pkTreeMap.put(new Row(key), s);
				// System.out.println(pkTreeMap.values());
				// System.out.println("size of map :: "+ pkTreeMap.size());
				Iterator it = secTreeMaps.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pairs = (Map.Entry) it.next();
					idxCols = (List<String>) pairs.getKey();
					secTempTreeMap = (PrimaryTreeMap<Row, List<Long>>) pairs
							.getValue();
					secColIndex = TupleStruct.getColPositions(
							TupleStruct.getColString(columnNameList), idxCols);
					Datum[] secKey = TupleStruct.getDatum(datum, secColIndex);
					Row seckeyob=new Row(secKey);
					if (secTempTreeMap.containsKey(seckeyob)) {
						// System.out.println("Key Already present");
						// printTuple(secKey);
						// printTuple(key);
						List<Long> tempRows = secTempTreeMap
								.get(seckeyob);
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
				i++;
				if (i > 10000) {
					i = 0;
					recman.commit();
					recman.clearCache();
				}
				datum = input.readOneTuple();
			}
			// System.out.println("Committing");
			recman.commit();
			// System.out.println("Clearing Cache");
			recman.clearCache();
			// System.out.println("Committing");
			recman.close();
			metaWriter.flush();
			metaWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
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
}
