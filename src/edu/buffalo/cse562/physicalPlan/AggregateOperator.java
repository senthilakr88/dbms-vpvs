package edu.buffalo.cse562.physicalPlan;

/*
 * to do
 * 1. pass the column name to the sum method
 * 2. loop readOneTuple method read all tuple as a datum[] from where (call readOneTuple method on select operator object)
 * 3. find the duplicate in the column (group by - column name) passed and compute the expression on the 
 * 4. Keep a map: key as the column name and value as the computed value (This has to be done as a buffer while reading the tuples. Ex: sum can be adding the values)
 * 
 */


import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.physicalPlan.Datum.dDate;
import edu.buffalo.cse562.physicalPlan.Datum.dLong;
import edu.buffalo.cse562.physicalPlan.Datum.dString;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

public class AggregateOperator implements Operator{
	// Select body and the groupby column name as the fields in the object that is passed to the visitor object
	Operator oper;
	SelectBody selectBody;
	List<Column> groupByColumnReferences;
	List<Column> tableColumnList;
	Datum aggregateSumDatum = null;
	
	public AggregateOperator(Operator oper,SelectBody selectBody, List<Column> tableColumnList){
		this.selectBody = selectBody;
		groupByColumnReferences = ((PlainSelect) selectBody).getGroupByColumnReferences();
		this.tableColumnList = tableColumnList;
		this.oper = oper;
	}
	
	
	@Override
	public void resetStream() {
		oper.resetStream();
		
	}

	@Override
	public Datum[] readOneTuple() {
		Datum[] readOneTupleFromOper = null;
		
		System.out.println("TEST PRINT!!!!!!");
		Datum[] test = this.oper.readOneTuple();
		printTuple(test);
		
		List<SelectItem> listOfSelectItems = ((PlainSelect) selectBody).getSelectItems();
		int count = 0;

		for(SelectItem itr:listOfSelectItems){
			System.out.println("FIRST FOR LOOP"+count);
			count++;
			SelectExpressionItem singleElement = (SelectExpressionItem) itr;	
			Expression aggregateExpression = singleElement.getExpression();
			Function aggregateFunction = (Function) aggregateExpression;
		
			ExpressionList funcParamExpressionList = aggregateFunction.getParameters();
			List<Column> funcParamList = funcParamExpressionList.getExpressions();
			String funcName = aggregateFunction.getName().toLowerCase().trim();
			Boolean isGroupByColumnSingleFlag = false;
			
			if(groupByColumnReferences.size()==1){
				isGroupByColumnSingleFlag = true;
			}
			
			switch(funcName){
			case "sum":
				System.out.println("SUM method");
				ArrayList<Column> sumParamArrayList = (ArrayList<Column>) funcParamList;
				String singleParamColumnName = "";
				int paramSize = sumParamArrayList.size();
				if(paramSize>1){
					for(Column itr1: sumParamArrayList){
						if(itr1.getColumnName().contains(".")){
							System.out.println("TABLE NAME.COLUMNNAME");
							String[] singleParam = itr1.getColumnName().split(".");
							if(singleParam.length>0){
								singleParamColumnName = singleParam[1];
							}
						}
						else{
							singleParamColumnName = itr1.getColumnName();
						}
						
						//call sum function
						System.out.println("Column name: "+singleParamColumnName);
						System.out.println("Aggregate function name:"+funcName);
						readOneTupleFromOper = sum(test, singleParamColumnName, isGroupByColumnSingleFlag);
					}
				}
				else{
					System.out.println("PRINT SINGLE FUNC PARAMETER");
					singleParamColumnName = sumParamArrayList.get(0).getColumnName();
					readOneTupleFromOper = sum(test, singleParamColumnName, isGroupByColumnSingleFlag);
				}
				
				break;
			case "count":
				System.out.println("COUNT method");
				break;
			case "min":
				System.out.println("MIN method");
				break;
			case "max":
				System.out.println("MAX method");
				break;
			case "avg":
				System.out.println("AVG method");
				break;
			case "stdev":
				System.out.println("STDEV method");
				break;
			default:
				System.out.println("AGGREGATE FUNCTION NOT MATCHED");
				break;
			}
		
		}
		return readOneTupleFromOper;
	}
	
	/*
	 * to do - compute sum of the values stored (singleParamColumnName) in a column within a table (singleParamTableName)
	 * input - table and column name
	 * compute - read the tuple (Datum[]) after where i.e. after checking the where condition
	 */
	public Datum[] sum(Datum[] readOneTupleFromOper,String singleParamColumnName, Boolean isGroupByColumnSingleFlag){
		Datum matchDatum=null;
		if(isGroupByColumnSingleFlag==true){
			for(int i=0;i<readOneTupleFromOper.length;i++){
				Datum singleTupleElement = readOneTupleFromOper[i];
				dLong datumInLong;
				dString datumInString;
				dDate datumInDate;
				System.out.println("OUT LOOP COUNT: "+i);
				if(singleTupleElement instanceof dLong){
					datumInLong = (dLong) singleTupleElement;
					if(datumInLong.getColumn().getColumnName().equals(singleParamColumnName)){
						matchDatum = datumInLong;
						System.out.println("MATCH DATUM: "+matchDatum);
						System.out.println("LOOP COUNT"+i);
						dLong aggregatedLongSumDatum = null;
						if(aggregateSumDatum != null){
							aggregatedLongSumDatum = (dLong) aggregateSumDatum;
							aggregateSumDatum = datumInLong.sumDatum(aggregatedLongSumDatum);
						}
						else{
							aggregateSumDatum = datumInLong;
						}
						dLong temp = (dLong) aggregateSumDatum;
						System.out.println("REPLACE AGE: "+temp);
						break;
					}
					continue;
				}
				else if(singleTupleElement instanceof dString){
					datumInString = (dString) singleTupleElement;
					if(datumInString.getColumn().equals(singleParamColumnName)){
						//aggregateSumDatum = datumInString.sumDatum(aggregateSumDatum);
						matchDatum = datumInString;
						break;
					}
					continue;
				}
				else if(singleTupleElement instanceof dString){
					datumInDate = (dDate) singleTupleElement;
					if(datumInDate.getColumn().equals(singleParamColumnName)){
						//aggregateSumDatum = datumInDate.sumDatum(aggregateSumDatum);
						matchDatum = datumInDate;
						break;
					}
					continue;
				}	
			}			
			for (int i=0; i<readOneTupleFromOper.length;i++) {
			    if (readOneTupleFromOper[i].equals(matchDatum)) { 
			    	readOneTupleFromOper[i] = aggregateSumDatum;
			        break;
			    }
			}	
			printTuple(readOneTupleFromOper);
		}
		else{
			//list of parameters - to do
		}
		return readOneTupleFromOper;
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

	