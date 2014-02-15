package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Tuple {
	Map<String,String> tableMap;
	
	public Tuple(Map<String,String> tableMap){
		this.tableMap = tableMap;
	}
	
	public String toString(){
		return tableMap.toString();
	}
	
}
