package edu.buffalo.cse562.utilities;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import jdbm.PrimaryHashMap;
import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.indexlonglistserializable;
import edu.buffalo.cse562.structure.indexlongserializer;
import edu.buffalo.cse562.structure.indexrowlistserializer;
import edu.buffalo.cse562.structure.indexrowserializer;
import edu.buffalo.cse562.structure.Datum.Row;

public class indexlookuptreemap {

	public static void main(String[] args) {
		String indexDir = "indexDir";
		String tableName = "orders";
		String idxName = "orderkey";
		ArrayList<Column> cols = new ArrayList<Column>();
		ArrayList<String> type = new ArrayList<String>();
//		 cols.add(new Column(new Table(tableName, tableName), "orderkey"));
//		 cols.add(new Column(new Table(tableName, tableName), "partkey"));
//		 cols.add(new Column(new Table(tableName, tableName), "suppkey"));
//		 cols.add(new Column(new Table(tableName, tableName), "linenumber"));
//		 cols.add(new Column(new Table(tableName, tableName), "quantity"));
//		 cols.add(new Column(new Table(tableName, tableName),
//		 "extendedprice"));
//		 cols.add(new Column(new Table(tableName, tableName), "discount"));
//		 cols.add(new Column(new Table(tableName, tableName), "tax"));
//		 cols.add(new Column(new Table(tableName, tableName), "returnflag"));
//		 cols.add(new Column(new Table(tableName, tableName), "linestatus"));
//		 cols.add(new Column(new Table(tableName, tableName), "shipdate"));
//		 cols.add(new Column(new Table(tableName, tableName), "commitdate"));
//		 cols.add(new Column(new Table(tableName, tableName), "receiptdate"));
//		 cols.add(new Column(new Table(tableName, tableName),
//		 "shipinstruct"));
//		 cols.add(new Column(new Table(tableName, tableName), "shipmode"));
//		 cols.add(new Column(new Table(tableName, tableName), "comment"));
		//
//		 type.add("INT");
//		 type.add("INT");
//		 type.add("INT");
//		 type.add("INT");
//		 type.add("DECIMAL");
//		 type.add("DECIMAL");
//		 type.add("DECIMAL");
//		 type.add("DECIMAL");
//		 type.add("CHAR(1)");
//		 type.add("CHAR(1)");
//		 type.add("DATE");
//		 type.add("DATE");
//		 type.add("DATE");
//		 type.add("CHAR(25)");
//		 type.add("CHAR(10)");
//		 type.add("VARCHAR(44)");

		// Parts
//		cols.add(new Column(new Table(tableName, tableName), "partkey"));
//		cols.add(new Column(new Table(tableName, tableName), "name"));
//		cols.add(new Column(new Table(tableName, tableName), "mfgr"));
//		cols.add(new Column(new Table(tableName, tableName), "brand"));
//		cols.add(new Column(new Table(tableName, tableName), "type"));
//		cols.add(new Column(new Table(tableName, tableName), "size"));
//		cols.add(new Column(new Table(tableName, tableName), "container"));
//		cols.add(new Column(new Table(tableName, tableName), "retailprice"));
//		cols.add(new Column(new Table(tableName, tableName), "comment"));
//		type.add("INT");
//		type.add("VARCHAR(55)");
//		type.add("CHAR(25)");
//		type.add("CHAR(10)");
//		type.add("VARCHAR(25)");
//		type.add("INT");
//		type.add("CHAR(10)");
//		type.add("DECIMAL");
//		type.add("VARCHAR(23)");
		
		
		cols.add(new Column(new Table(tableName, tableName), "orderkey"));
		cols.add(new Column(new Table(tableName, tableName), "custkey"));
		cols.add(new Column(new Table(tableName, tableName), "orderstatus"));
		cols.add(new Column(new Table(tableName, tableName), "totalprice"));
		cols.add(new Column(new Table(tableName, tableName), "orderdate"));
		cols.add(new Column(new Table(tableName, tableName), "orderpriority"));
		cols.add(new Column(new Table(tableName, tableName), "clerk"));
		cols.add(new Column(new Table(tableName, tableName), "shippriority"));
		cols.add(new Column(new Table(tableName, tableName), "comment"));
		type.add("INT");
		type.add("INT");
		type.add("CHAR(1)");
		type.add("DECIMAL");
		type.add("DATE");
		type.add("CHAR(15)");
		type.add("CHAR(15)");
		type.add("INT");
		type.add("VARCHAR(79)");
		System.out.println("size of cols :: " + cols.size());
		System.out.println("Size of type :: " + type.size());
		PrimaryStoreMap<Long, Row> pkStoreMap = null;
		PrimaryTreeMap<Row, Long> pkHashMap = null;
		PrimaryTreeMap<Row, List<Long>> secMap = null;
		try {
			RecordManager recman = RecordManagerFactory
					.createRecordManager(indexDir + File.separator + tableName);

			List<String> idxCols = new ArrayList<String>();
			idxCols.add("orderkey");
//			idxCols.add("linenumber");
			List<Integer> pkColIndex = TupleStruct.getColPositions(
					TupleStruct.getColString(cols), idxCols);
			System.out.println(pkColIndex);
			indexrowserializer pkIndexSer = new indexrowserializer(
					TupleStruct.getShrList(cols, pkColIndex),
					TupleStruct.getShrList(type, pkColIndex));
			indexrowserializer datumIndexSer = new indexrowserializer(cols,
					type);
			indexlongserializer pkLongSer = new indexlongserializer();
			
			pkStoreMap = recman.storeMap(tableName+"map",datumIndexSer);
//			System.out.println("Size of Primary Map :: " + pkStoreMap.size());
//			System.out.println(pkStoreMap.keySet());
//			System.out.println(pkStoreMap.values());
			System.out.println("tableName :: "+tableName);
			pkHashMap = recman.treeMap(tableName, pkLongSer, pkIndexSer);
			System.out.println("Size of Primary Hash Map :: " + pkHashMap.size());
			System.out.println(pkHashMap.keySet());
			Datum[] datum = new Datum[1];
			datum[0] = new Datum.dLong("775", null);
//			System.out.println(pkHashMap.get(new Row(datum)));
			System.out.println(pkHashMap.values());

			List<String> idxCols1 = new ArrayList<String>();
			idxCols1.add("shipdate");
			List<Integer> secColIndex = TupleStruct.getColPositions(
					TupleStruct.getColString(cols), idxCols1);
			System.out.println(secColIndex);
			indexrowserializer secIndexSer = new indexrowserializer(
					TupleStruct.getShrList(cols, secColIndex),
					TupleStruct.getShrList(type, secColIndex));

			indexrowlistserializer pkIndexSerList = new indexrowlistserializer(
					TupleStruct.getShrList(cols, pkColIndex),
					TupleStruct.getShrList(type, pkColIndex));

			indexlonglistserializable pkLongSerList = new indexlonglistserializable();
			
			secMap = recman.treeMap(idxName, pkLongSerList, secIndexSer);
			System.out.println("Size of Secondary Map :: " + secMap.size());
			System.out.println(secMap.keySet());
			System.out.println(secMap.values());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
