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
	private List<Column> tableMap;
	private Map<String, ArrayList<String>> tableColTypeMap;
	BufferedReader reader;
	Table tableName;
	String tablefile;

	public FileScanOperator(Table tableName, String dirName,
			List<Column> tableMap2,
			Map<String, ArrayList<String>> tableColTypeMap) {
		this.dirName = dirName;
		this.tableMap = tableMap2;
		this.tableName = tableName;
		this.tableColTypeMap = tableColTypeMap;
		tablefile = new File("").getAbsolutePath() + dirName
					+ File.separator + tableName + ".dat";
			
		resetStream();
	}

	/*
	 * @author - vino logic - construct tuple object with map containing key as
	 * column name (ArrayList<String>) and value as table data
	 * (ArrayList<String>)
	 */
	@Override
	public Datum[] readOneTuple() {
		if(reader == null) {
			System.out.println("Buffer not initialized for table ::" + tableName);
			return null;
		}
		Datum[] oneTupleFromDat = null;
		try {
			String line = null;
			if ((line = reader.readLine()) != null) {

				String[] singleTableElement = line.split("\\|");
				
				//oneTupleFromDat = new Tuple(convertType(singleTableElement));
				oneTupleFromDat = convertType(singleTableElement);
			} else {
				
				return null;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return oneTupleFromDat;
	}

	public Datum[] convertType(String[] singleTableElement) {
		//Map tupleKeyValueMap = new HashMap();
		String key = null, value = null, type = null;
		Column col = null;
		Datum[] t = new Datum[singleTableElement.length];
		int i = 0;
		while (i < singleTableElement.length) {
			//key = tableMap.get.get(tableName).get(i);
			if(tableMap.get(i).getTable().toString().equalsIgnoreCase(tableName.toString())) {
				key = tableMap.get(i).getColumnName();
				col = tableMap.get(i);
			}
			else
				continue;
			value = singleTableElement[i];
			type = tableColTypeMap.get(tableName.toString()).get(i);
			if (type.equalsIgnoreCase("int"))
//				tupleKeyValueMap.put(key, Integer.parseInt(value));
				t[i] = new Datum.dLong(singleTableElement[i],col);
			else if (type.equalsIgnoreCase("String")) {
//				tupleKeyValueMap.put(key, value);
				t[i] = new Datum.dString(singleTableElement[i],col);
			} else if (type.equalsIgnoreCase("date")) {
				//		tupleKeyValueMap.put(key, (new SimpleDateFormat(
//							"YYYY-MM-DD", Locale.ENGLISH).parse(value)));
					t[i] = new Datum.dDate(singleTableElement[i],col);
				
			} else {
				try {
					throw new Exception("Not aware of this data type :: "+ type);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			i++;
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
}
