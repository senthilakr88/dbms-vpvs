package edu.buffalo.cse562.physicalPlan;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FileScanOperator implements Operator{
	private String fileName;
	private Map<String,ArrayList<String>> tableMap;
	BufferedReader reader;
	String tableName;
	
	public FileScanOperator(String tableName, String fileName, Map<String,ArrayList<String>> tableMap){
		this.fileName = fileName;
		this.tableMap = tableMap;
		this.tableName = tableName;
		try {
			reader = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/*@author - vino
	 * logic - construct tuple object with map containing key as column name (ArrayList<String>) and value as table data (ArrayList<String>)
	 */
	@Override
	public Tuple readOneTuple() {
		Tuple oneTupleFromDat = null;
		try {
			String line = null;
			if ((line = reader.readLine()) != null) {
				Map<String,String> tupleKeyValueMap = new HashMap<String,String>();
				String[] singleTableElement = line.split("\\|");
			    int i=0;
				while (i<singleTableElement.length) {
			        tupleKeyValueMap.put(tableMap.get(tableName).get(i),singleTableElement[i]);
			        i++;
			    }
				oneTupleFromDat = new Tuple(tupleKeyValueMap);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return oneTupleFromDat;
	}

	@Override
	public void resetStream() {
		// TODO Auto-generated method stub
		
	}
}
