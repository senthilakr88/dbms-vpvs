package edu.buffalo.cse562.physicalPlan;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.buffalo.cse562.physicalPlan.Datum.dDate;
import edu.buffalo.cse562.physicalPlan.Datum.dLong;
import edu.buffalo.cse562.physicalPlan.Datum.dString;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupbyOperator {
	Operator oper;
	ArrayList<SelectExpressionItem> selectExpressionList;
	List<Column> groupbyList;
	Map<String,ArrayList<Datum[]>> groupByMap = new HashMap<String,ArrayList<Datum[]>>();
	boolean isTupleMapPresent;
	Test test;
	
	public GroupbyOperator(Operator oper,ArrayList<SelectExpressionItem> selectExpressionList,Test test,List<Column> groupbyList){
		this.oper = oper;
		this.groupbyList = groupbyList;
		this.test = test;
		this.isTupleMapPresent = true;
		this.selectExpressionList = selectExpressionList;
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
		ArrayList<Column> groupbyArrayList= (ArrayList<Column>) groupbyList;
		ArrayList<Function> aggregareFunctionList = new ArrayList<Function>();
		Datum[] readOneTupleFromOper = oper.readOneTuple();
		printTuple(readOneTupleFromOper);
		Datum singleDatum; 
		//System.out.println("TTEEEEEEE");
		int countExpression = 1;
		
		if(isTupleMapPresent) {
			TupleStruct.setTupleTableMap(readOneTupleFromOper);
			isTupleMapPresent = false;
		}
		
		int count = 0;
		ArrayList<String> datumColumnName = (ArrayList<String>) TupleStruct.getTupleTableMap();
		while(readOneTupleFromOper != null){
			System.out.println("NEW TUPLE READ");
			count++;
			StringBuilder mapKey = new StringBuilder();
			for(Column groupbyColumnName : groupbyList){
				if(datumColumnName.contains(groupbyColumnName.getWholeColumnName().toLowerCase())){
					//System.out.println("TESTTTTTT");
					int index = datumColumnName.indexOf(groupbyColumnName.getWholeColumnName().toLowerCase());
					printTuple(readOneTupleFromOper);
					System.out.println(index);
					singleDatum = readOneTupleFromOper[index];
					//System.out.println("GROUP BY COLUMN NAME/VALUE: "+singleDatum.getColumn().getColumnName().toLowerCase()+"/"+singleDatum.getStringValue());
					mapKey.append(singleDatum.getStringValue());
					System.out.println(mapKey);
				}
				else{
					
				}
			}
			
			//Building the datum[] from select item expressions
			Datum[] newSelectItemsArray= new Datum [selectExpressionList.size()];
			
			
			for(int itr=0;itr<selectExpressionList.size();itr++){
				System.out.println("EXPRESSION"+countExpression);
				SelectExpressionItem newItem = selectExpressionList.get(itr);
				Expression e = newItem.getExpression();
				
				CalcTools calc = new CalcTools(readOneTupleFromOper); 
				
				if(e instanceof Function){
					System.out.println("PRINT THERE IS A FUNCTION IN THE SELECT BODY");
					Function aggregateFunction = (Function) e;
					aggregareFunctionList.add(aggregateFunction);
					String funcName = aggregateFunction.getName();
					
					ExpressionList funcParamExpressionList = aggregateFunction.getParameters();
					List<Expression> funcParamList = funcParamExpressionList.getExpressions();
					
					//System.out.println("Expressssss"+e);
					
					e.accept(calc);
					Object calcOut = calc.getResult();
					
					Column newCol = null;
					Datum tempDatum = null;
					if (newItem.getAlias()!=null){
						//System.out.println("Alias");
						newCol = new Column(null, newItem.getAlias());
					}
					else {
						//System.out.println("Exp");
						newCol = calc.getColumn();
						//System.out.println(newCol.getColumnName());
					}
					
					if(calcOut instanceof Long){
						String value = calcOut.toString();
						String valueWithFuncName = funcName.concat("("+value+")");
						tempDatum = new Datum.dLong(calcOut.toString(), newCol);
						//System.out.println("CALC OUTPUT FOR dLONG: "+calcOut.toString());
//						System.out.println("CALC OUTPUT FOR dLONG: "+newCol);
						//System.out.println("NEW FUNC VALUE: "+valueWithFuncName);
						//System.out.println("tempDatum: "+tempDatum.getStringValue());
					}
					else if(calcOut instanceof String){
						tempDatum = new Datum.dString((String) calcOut, newCol);
						//System.out.println("CALC OUTPUT FOR DSTRING: "+(String) calcOut);
						//System.out.println("tempDatum"+tempDatum.getStringValue());
					}
					else if(calcOut instanceof Date){
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
						tempDatum = new Datum.dDate(df.format(calcOut), newCol);
						//System.out.println("tempDatum"+tempDatum.getStringValue());
						
					}
					else if(calcOut instanceof Double){
						Double value = (Double) calcOut;
					}
						
					//System.out.println("CALC OUTPUT: "+ calcOut);
					newSelectItemsArray[itr] = tempDatum;
					countExpression++;
					
					
				}
				else {
					e.accept(calc);
					Object calcOut = calc.getResult();
					
					Column newCol = null;
					Datum tempDatum = null;
					if (newItem.getAlias()!=null){
						newCol = new Column(null, newItem.getAlias());
					}
					else {
						newCol = calc.getColumn();
					}
					
					if(calcOut instanceof Long){
						tempDatum = new Datum.dLong(calcOut.toString(), newCol);
						//System.out.println("tempDatum: "+tempDatum.getStringValue());
						//System.out.println("new datum column name: "+newCol.getColumnName());
					}
					else if(calcOut instanceof String){
						tempDatum = new Datum.dString((String) calcOut, newCol);
						//System.out.println("new datum column name: "+newCol.getColumnName());
						//System.out.println("tempDatum"+tempDatum.getStringValue());
					}
					else if(calcOut instanceof Date){
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
						tempDatum = new Datum.dDate(df.format(calcOut), newCol);
						//System.out.println("new datum column name: "+newCol.getColumnName());
						//System.out.println("tempDatum"+tempDatum.getStringValue());
						
					}
					else if(calcOut instanceof Double){
						Double value = (Double) calcOut;
					}
						
					System.out.println("CALC OUTPUT: "+ calcOut);
					newSelectItemsArray[itr] = tempDatum;
					countExpression++;
					System.out.println("NEW TUPLE: "+newSelectItemsArray[itr] .getColumn()+"VALUE: "+newSelectItemsArray[itr] .getStringValue());
				}
				
			}
			
			//System.out.println("PRINT NEW TUPLE ARRAY: ");
			//printTuple(newSelectItemsArray);
			
			//Populating the first entry of the map
			if(groupByMap.size()<1){
				System.out.println("ADDING FIRST KEY/VALUE PAIR OF THE MAP");
				ArrayList<Datum[]> datumArrayList = new ArrayList<Datum[]>();
				datumArrayList.add(newSelectItemsArray);
				groupByMap.put(mapKey.toString(), datumArrayList);
				System.out.println(groupByMap.toString());
				continue;
			}
			//Adding a new entry to the map
			else{
				if(!groupByMap.containsKey(mapKey.toString())){
					System.out.println("ADDING NEW KEY/VALUE PAIR TO THE MAP");
					ArrayList<Datum[]> datumArrayList = new ArrayList<Datum[]>();
					datumArrayList.add(newSelectItemsArray);
					groupByMap.put(mapKey.toString(), datumArrayList);
				}
				else{
					System.out.println("KEY/VALUE PAIR ALREADY EXISTING IN MAP");
					ArrayList<Datum[]> datumArrayList = groupByMap.get(mapKey.toString());
					datumArrayList.add(newSelectItemsArray);
					groupByMap.put(mapKey.toString(), datumArrayList);
					
				}
			}
					
					
				
					/*if(groupByMap.size()<1){
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
					}*/
					
			
				
			readOneTupleFromOper = this.oper.readOneTuple();
		}
		System.out.println("SIZE OF THE MAP"+groupByMap.size());
		for (Entry<String, ArrayList<Datum[]>> entry : groupByMap.entrySet()) {
		    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()+"value size: "+entry.getValue().size());
		}
		
		//Iterate over the map to get the key; for each key, iterate over the value i.e. ArrayList<Datum[]>, get the datum[] and pass it to aggregate operator
		//create Aggregate operator
		for (Entry<String, ArrayList<Datum[]>> entry : groupByMap.entrySet()) {
			ArrayList<Datum[]> MapValueList = entry.getValue();
			for(Datum[] singleDatumArray:MapValueList){
				System.out.println("PRINTING DATUM[] before passing to TEST FUCNTION");
				printTuple(singleDatumArray);
				finalGroupByDatumArrayList.add(test.aggregateFunction(singleDatumArray, aggregareFunctionList));
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
