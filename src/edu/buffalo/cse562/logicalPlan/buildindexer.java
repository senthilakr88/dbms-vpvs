package edu.buffalo.cse562.logicalPlan;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;
import edu.buffalo.cse562.physicalPlan.FileScanOperator;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.structure.Datum;
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
	List<Index> indexes;

	public buildindexer(String tableDir, String indexDir, String preCompDir) {
		this.tableDir = tableDir;
		this.indexDir = indexDir;
		this.preCompDir = preCompDir;
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
		buildIndex();
	}

	public static class seckeyExtract<Row> implements
			SecondaryKeyExtractor<Row, Row, Row> {

		List<Integer> colPos;

		public seckeyExtract(List<Integer> colPos) {
			this.colPos = colPos;
		}

		@Override
		public Row extractSecondaryKey(Row key, Row value) {
			Row row = (Row) new Datum.Row(TupleStruct.getDatum(((Datum.Row)value).getDatum(), colPos)); 
			return row;
		}
	}

	public void buildIndex() {
		Index idx;
		List<String> idxCols;
		String idxType;
		RecordManager recman;
		String idxName;
		PrimaryTreeMap<Row, Row> pkTreeMap = null;

		List<Integer> pkColIndex = null;
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
				if (idxType.contains("PRIMARY")) {
					pkTreeMap = recman.treeMap(idxName);
					pkColIndex = TupleStruct
							.getColPositions(
									TupleStruct.getColString(columnNameList),
									idxCols);
				} else {
					SecondaryTreeMap<Row, Row, Row> secMap = pkTreeMap
							.secondaryTreeMap(idxName, new seckeyExtract<Row>(
									TupleStruct
									.getColPositions(
											TupleStruct.getColString(columnNameList),
											idxCols)));
				}
			}
			input = new FileScanOperator(table, tableDir, columnNameList,
					columnTypeList, null);
			int i = 0;
			Datum[] datum = input.readOneTuple();
			while (datum != null) {
				Datum[] key = TupleStruct.getDatum(datum, pkColIndex);
				pkTreeMap.put(new Row(key), new Row(datum));
				i++;
				if (i > 10000) {
					i = 0;
					recman.commit();
					recman.clearCache();
				}
				datum = input.readOneTuple();
			}
			recman.commit();
			recman.clearCache();
			pkTreeMap.clear();
			recman.close();
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
