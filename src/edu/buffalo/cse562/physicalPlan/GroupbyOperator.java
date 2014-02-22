package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.buffalo.cse562.physicalPlan.Datum.dDate;
import edu.buffalo.cse562.physicalPlan.Datum.dLong;
import edu.buffalo.cse562.physicalPlan.Datum.dString;
import net.sf.jsqlparser.schema.Column;

public class GroupbyOperator {
	Operator oper;
	List<Column> groupbyList;
	Map<Datum,ArrayList<Datum[]>> groupByMap = new HashMap<Datum,ArrayList<Datum[]>>();
	
	Test test;

	public GroupbyOperator(Operator oper, Test test,List<Column> groupbyList){
		this.oper = oper;
		this.groupbyList = groupbyList;
		this.test = test;
		
	}

	public void resetStream() {
		// TODO Auto-generated method stub
		
	}


	/*
	 * read one tuple from the operator, iterate over the column
	 * 
	 */
	public ArrayList<Datum[]> readOneTuple() {
		ArrayList<Datum[]> finalGroupByDatumArrayList = new ArrayList<Datum[]>();
		String groupByColumnName = "";
		ArrayList<Column> groupbyArrayList= (ArrayList<Column>) groupbyList;
		
		if(groupbyArrayList.size()<=1){
			groupByColumnName = groupbyArrayList.get(0).getColumnName();
		}
		System.out.println("GROUPBY COLUMN NAME: "+groupByColumnName);	
		Datum[] readOneTupleFromOper = oper.readOneTuple();
		//oper.resetStream();
		int count = 0;
		while(readOneTupleFromOper != null){
			count++;
			for(Datum singleDatum:readOneTupleFromOper){
				if(singleDatum.getColumn().getColumnName().equalsIgnoreCase(groupByColumnName)){
					System.out.println("MATCH");
					
					if(groupByMap.size()<1){
						System.out.println("INSIDE GROUPBY MAP INIT");
						ArrayList<Datum[]> datumArrayList = new ArrayList<Datum[]>();
						datumArrayList.add(readOneTupleFromOper);
						System.out.println(singleDatum.toComString());

						groupByMap.put(singleDatum, datumArrayList);
						System.out.println(groupByMap.toString());
						continue;
					}
					System.out.println(singleDatum.toComString());
									
					if(singleDatum instanceof dString){
						System.out.println("single datum - dString format");
						System.out.println("<-------Gender Datum---->");
						System.out.println(((dString) singleDatum).getValue());
						
						if(groupByMap.containsKey(singleDatum)){
							System.out.println("IF CONDITION - SINGLE DATUM KEY FOUND IN MAP");
							ArrayList<Datum[]> retrieveDatumList = groupByMap.get(singleDatum);	
							retrieveDatumList.add(readOneTupleFromOper);
					    	groupByMap.put(singleDatum, retrieveDatumList);
						}
						else{
							System.out.println("ELSE CONDITION - SINGLE DATUM KEY NOT FOUND IN MAP");
							ArrayList<Datum[]> datumArrayList = new ArrayList<Datum[]>();
							datumArrayList.add(readOneTupleFromOper);
							groupByMap.put(singleDatum, datumArrayList);
						}	
					}
					
					else if(singleDatum instanceof dLong){
						System.out.println("single datum - dLong format");
						if(groupByMap.containsKey(singleDatum)){
							System.out.println("IF CONDITION - SINGLE DATUM KEY FOUND IN MAP");
							ArrayList<Datum[]> retrieveDatumList = groupByMap.get(singleDatum);
							retrieveDatumList.add(readOneTupleFromOper);
					    	groupByMap.put(singleDatum, retrieveDatumList);
						}
						else{
							System.out.println("ELSE CONDITION - SINGLE DATUM KEY NOT FOUND IN MAP");
							ArrayList<Datum[]> datumArrayList = new ArrayList<Datum[]>();
							datumArrayList.add(readOneTupleFromOper);
							groupByMap.put(singleDatum, datumArrayList);
						}
						
					}
					else if(singleDatum instanceof dDate){
						System.out.println("-----TO DO----");
						//groupByMap.put(singleDatum, retrieveDatumList);
					}
					
				}
			}
			readOneTupleFromOper = this.oper.readOneTuple();
		}
		System.out.println("SIZE OF THE MAP"+groupByMap.size());
		for (Entry<Datum, ArrayList<Datum[]>> entry : groupByMap.entrySet()) {
		    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()+"value size: "+entry.getValue().size());
		}
		
		//Iterate over the map to get the key; for each key, iterate over the value i.e. ArrayList<Datum[]>, get the datum[] and pass it to aggregate operator
		//create Aggregate operator
		for (Entry<Datum, ArrayList<Datum[]>> entry : groupByMap.entrySet()) {
			ArrayList<Datum[]> MapValueList = entry.getValue();
			for(Datum[] singleDatumArray:MapValueList){
				finalGroupByDatumArrayList.add(test.aggregateFunction(singleDatumArray));
				//Datum[] aggregateDatumArray = test.aggregateFunction(singleDatumArray);
				System.out.println("-------------------------------------------------");
				//printTuple(aggregateDatumArray);
			}
			test.resetAggregateDatumBuffer();
		    //System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()+"value size: "+entry.getValue().size());
		}
		
		
		return finalGroupByDatumArrayList;
	}
	
	private void printTuple(Datum[] row) {
		if(row!=null && row.length !=0) {
		for(Datum col : row) {
			System.out.print(col + "|");
		}
		System.out.println("");
		}
		System.out.println("------------------------------------------------");
	}

}
