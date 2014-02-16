package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Tuple {
	Map tableMap;
	
	public Tuple(Map tableMap){
		this.tableMap = tableMap;
	}
	
	public String toString(){
		return tableMap.toString();
	}
	public boolean contains(String str)
	{
		return tableMap.containsKey(str);
	}
	public String valueof(String str)
	{
		return tableMap.get(str);
	}
}
