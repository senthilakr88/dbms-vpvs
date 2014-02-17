package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Tuple {
	Map tableMap;
	
	public Tuple(Map tableMap){
		this.tableMap = tableMap;
	}
	
	public boolean contains(String key) {
		return tableMap.containsKey(key);
	}
	
	public String valueOf(String key) {
		return (String)tableMap.get(key);
	}
	
	public String toString(){
		String output ="";
		Iterator iterator = tableMap.entrySet().iterator();
		
		while(iterator.hasNext()) {
			Map.Entry mapEntry = (Map.Entry) iterator.next();
			output += mapEntry.getValue() +"|";
		}
		return output;
	}
}
