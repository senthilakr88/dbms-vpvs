package edu.buffalo.cse562.physicalPlan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class FileScanOperator implements Operator {
	private String dirName;
	private Map<String, ArrayList<Column>> tableMap;
	private Map<String, ArrayList<String>> tableColTypeMap;
	BufferedReader reader;
	Table tableName;
	String tablefile;
	List<Datum[]> buffer;
	Integer bufferMaxSize;
	Integer bufferPointer;
	Boolean isEnd;
	int count;
	int ColStartIndex;
	Map<String, ArrayList<Integer>> tableRemoveCols;
	String tempTableName;
	ArrayList<String> colType;
	ArrayList<Column> colList;
	ArrayList<Integer> removeCols;

	public FileScanOperator(Table tableName, String dirName,
			Map<String, ArrayList<Column>> tableMap2,
			Map<String, ArrayList<String>> tableColTypeMap, Map<String, ArrayList<Integer>> tableRemoveCols) {
		this.dirName = dirName;
		this.tableMap = tableMap2;
		this.tableName = tableName;
		this.tableColTypeMap = tableColTypeMap;
		String basePath = dirName + File.separator + tableName.getName()
				+ ".dat";
		if (!(new File(basePath).exists())) {
			tablefile = new File("").getAbsolutePath() + File.separator
					+ basePath;
		} else {
			tablefile = basePath;
		}
		resetStream();
		this.tableRemoveCols = tableRemoveCols;
		this.bufferMaxSize = 1000;
		this.bufferPointer = -1;
		this.buffer = new ArrayList<Datum[]>(bufferMaxSize);
		this.isEnd = false;
		this.ColStartIndex = 0;
		tempTableName = tableName.getName().toLowerCase();
		colType = tableColTypeMap.get(tempTableName);
		colList = tableMap.get(tempTableName);
		removeCols = tableRemoveCols.get(tempTableName);
	}

	/*
	 * @author - vino logic - construct tuple object with map containing key as
	 * column name (ArrayList<String>) and value as table data
	 * (ArrayList<String>)
	 */
	@Override
	public Datum[] readOneTuple() {
		if (reader == null) {
			System.out.println("Buffer not initialized for table ::"
					+ tableName);
			return null;
		}
		Datum[] oneTupleFromDat = null;
		if (bufferPointer.compareTo(buffer.size() - 1) != 0
				&& buffer.size() <= bufferMaxSize) {
			// System.out.println(bufferPointer);
			++bufferPointer;
//			 printTuple(buffer.get(bufferPointer));
			return buffer.get(bufferPointer);
		} else {
			bufferPointer = -1;
			buffer = new ArrayList<Datum[]>(bufferMaxSize);
			while (buffer.size() < bufferMaxSize && !isEnd) {
				try {

					String line = null;

					if ((line = reader.readLine()) != null) {
						if (line.equalsIgnoreCase("") || line.isEmpty()) {
							isEnd = true;
						}
						String[] singleTableElement = line.split("\\|");

						// oneTupleFromDat = new
						// Tuple(convertType(singleTableElement));
						oneTupleFromDat = convertType(singleTableElement);
						buffer.add(oneTupleFromDat);
					} else {

						isEnd = true;
					}

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (!isEnd) {
				++bufferPointer;
//				 printTuple(buffer.get(bufferPointer));
				return buffer.get(bufferPointer);
			} else {
				if (bufferPointer.compareTo(buffer.size() - 1) != 0) {
					// System.out.println(bufferPointer);
					++bufferPointer;
//					printTuple(buffer.get(bufferPointer));
					return buffer.get(bufferPointer);
				} else {
//					System.out.println("entered empty");
					isEnd = false;
					return null;
				}
			}
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

	public Datum[] convertType(String[] singleTableElement) {
		// Map tupleKeyValueMap = new HashMap();
		String key = null, value = null, type = null;
		Column col;		
		Datum[] t;
		int i = 0, k=0;
		int count = colList.size();
					
		t = new Datum[count];
		while (i < singleTableElement.length) {
			
			if(removeCols.contains(i)) {
				i++;
//				System.out.println("i :: "+ i + " k :: " + k +col.getColumnName() + " :: Not present continuing " );
				continue;
			}
//			System.out.println("i :: "+ i + " k :: "+k + " col ::" + col.getColumnName());
			value = singleTableElement[i];
			col = colList.get(k);
			type = colType.get(k).toLowerCase();
			if (type.equals("int")) {
				// tupleKeyValueMap.put(key, Integer.parseInt(value));
				t[k] = new Datum.dLong(singleTableElement[i], new Column(
						tableName, col.getColumnName()));
				// System.out.print(t[i].toComString());
			} else if (type.equals("decimal")) {
				// tupleKeyValueMap.put(key, Integer.parseInt(value));
				t[k] = new Datum.dDecimal(singleTableElement[i], new Column(
						tableName, col.getColumnName()),4);
				// System.out.print(t[i].toComString());
			} else if (type.equals("string")
					|| type.startsWith("char") || type.startsWith("varchar")) {
				// tupleKeyValueMap.put(key, value);
				t[k] = new Datum.dString(singleTableElement[i], new Column(
						tableName, col.getColumnName()));
				// System.out.print(t[i].toComString());
			} else if (type.equals("date")) {
				// tupleKeyValueMap.put(key, (new SimpleDateFormat(
				// "YYYY-MM-DD", Locale.ENGLISH).parse(value)));
				t[k] = new Datum.dDate(singleTableElement[i], new Column(
						tableName, col.getColumnName()));
				// System.out.print(t[i].toComString());
			} else {
				try {
					throw new Exception("Not aware of this data type :: "
							+ type);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			i++;
			k++;
//			System.out.println("updating i, j, k");
		}
		return t;

	}

	@Override
	public void resetStream() {
		try {
			reader = new BufferedReader(new FileReader(tablefile));
		} catch (IOException e) {

			e.printStackTrace();
			reader = null;
		}

	}

	@Override
	public void resetTupleMapping() {
		return;
		
	}
}
