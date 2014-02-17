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
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

public class FileScanOperator implements Operator {
	private String dirName;
	private Map<String, ArrayList<String>> tableMap, tableColTypeMap;
	BufferedReader reader;
	String tableName;
	String tablefile;

	public FileScanOperator(String tableName, String dirName,
			Map<String, ArrayList<String>> tableMap,
			Map<String, ArrayList<String>> tableColTypeMap) {
		this.dirName = dirName;
		this.tableMap = tableMap;
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
	public Tuple readOneTuple() {
		if(reader == null) {
			System.out.println("Buffer not initialized for table ::" + tableName);
			return null;
		}
		Tuple oneTupleFromDat = null;
		try {
			String line = null;
			if ((line = reader.readLine()) != null) {

				String[] singleTableElement = line.split("\\|");

				oneTupleFromDat = new Tuple(convertType(singleTableElement));
			} else {
				reader.close();
				reader = null;
				return null;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return oneTupleFromDat;
	}

	public Map convertType(String[] singleTableElement) {
		Map tupleKeyValueMap = new HashMap();
		String key = null, value = null, type = null;
		int i = 0;
		while (i < singleTableElement.length) {
			key = tableMap.get(tableName).get(i);
			value = singleTableElement[i];
			type = tableColTypeMap.get(tableName).get(i);
			if (type.equalsIgnoreCase("int"))
				tupleKeyValueMap.put(key, Integer.parseInt(value));
			else if (type.equalsIgnoreCase("String")) {
				tupleKeyValueMap.put(key, value);
			} else if (type.equalsIgnoreCase("date"))
				try {
					tupleKeyValueMap.put(key, (new SimpleDateFormat(
							"YYYY-MM-DD", Locale.ENGLISH).parse(value)));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			else {
				try {
					throw new Exception("");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			i++;
		}
		return tupleKeyValueMap;

	}

	@Override
	public void resetStream() {
		try {
			reader = new BufferedReader(new FileReader(tablefile));
		} catch (IOException e) {
			reader = null;
			e.printStackTrace();
		}

	}
}
