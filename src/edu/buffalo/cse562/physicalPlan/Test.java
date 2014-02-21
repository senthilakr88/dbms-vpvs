package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Test implements ExpressionVisitor{
	//Test object stores the groupby column names as it's field
	List<Column> groupByColumnReferences;
	Operator oper;
	
	//Maintain these buffer values specific for individual Test objects
	Datum aggregateSumDatum = null;
	Datum[] readOneTupleFromOper;
	
	public Test(Operator oper,List<Column> groupByColumnReferences, List<Column> tableColumnList) {
		this.groupByColumnReferences = groupByColumnReferences;
		this.oper = oper;
	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}
	
	/*
	 * This function is the aggregate function such as MAX(A), COUNT(B), MIN(TABLENAME.C)
	 * To do: get the function name and list of parameters;
	 * input - aggregate function containing the function name and function parameters
	 * output -  method call to sum function; single group by column and single group by list
	 */
	@Override
	public void visit(Function aggregateFunc) {
		ExpressionList funcParamExpressionList = aggregateFunc.getParameters();
		List<Column> funcParamList = funcParamExpressionList.getExpressions();
		String funcName = aggregateFunc.getName().toLowerCase().trim();
		Boolean isGroupByColumnSingleFlag = false;
		
		//check whether the group by column is a single column and set the flag
		if(groupByColumnReferences.size()==1){
			isGroupByColumnSingleFlag = true;
		}
		
		//Check the aggregate function name and call the function appropriately
		switch(funcName){
		case "sum":
			System.out.println("SUM method");
			ArrayList<Column> sumParamArrayList = (ArrayList<Column>) funcParamList;
			
			//iterate over the parameter list and call function for each parameter 
			for(Column itr: sumParamArrayList){
				String singleParamColumnName = "";
				
				//check if the aggregate function parameters contain TABLE_NAME.COLUMN_NAME format i.e. we can get the table name in such case
				if(itr.getColumnName().contains(".")){
					System.out.println("TABLE NAME.COLUMNNAME");
					String[] singleParam = itr.getColumnName().split(".");
					if(singleParam.length>0){
						singleParamColumnName = singleParam[1];
					}
				}
				else{
					singleParamColumnName = itr.getColumnName();
				}
				
				//call sum function
				System.out.println("Column name: "+singleParamColumnName);
				System.out.println("Aggregate function name:"+funcName);
				sum(singleParamColumnName, isGroupByColumnSingleFlag);
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
	
	/*
	 * to do - compute sum of the values stored (singleParamColumnName) in a column within a table (singleParamTableName)
	 * input - table and column name
	 * compute - read the tuple (Datum[]) after where i.e. after checking the where condition
	 */
	public void sum(String singleParamColumnName, Boolean isGroupByColumnSingleFlag){
		
		String groupByColumnName = "";
		Boolean DatumMatch = false;
		FileScanOperator fileScanOper;
		SelectionOperator selOper;
		JoinOperator joinOper;
		ProjectionOperator projOper;
		Datum matchDatum=null;
		Boolean isGroupByTupleMatch = false;
		Boolean isGroupByColumnFlag = false;

		//Logic for populating the datum[] for a single tuple from the Operator object
		if(oper instanceof FileScanOperator){
			fileScanOper = (FileScanOperator) oper;
			readOneTupleFromOper = new Datum[fileScanOper.readOneTuple().length];
			readOneTupleFromOper = fileScanOper.readOneTuple();
		}
		else if(oper instanceof SelectionOperator){
			selOper = (SelectionOperator) oper;
			//Expression condition =((SelectionOperator) oper).condition;
			//selOper = new SelectionOperator(oper.,condition);
			System.out.println("PRINT LENGTH"+ selOper.readOneTuple().length);
			//int size = selOper.readOneTuple().length;
			//readOneTupleFromOper = new Datum[size];
			readOneTupleFromOper = selOper.readOneTuple();
			System.out.println("SELECT OPERATOR");
			System.out.println("READONETUPLE LENGTH: "+readOneTupleFromOper.length);
		}
		else if(oper instanceof JoinOperator){
			joinOper = (JoinOperator) oper;
			readOneTupleFromOper = new Datum[joinOper.readOneTuple().length];
			readOneTupleFromOper = joinOper.readOneTuple();
		}
		else if(oper instanceof ProjectionOperator){
			projOper = (ProjectionOperator) oper;
			readOneTupleFromOper = new Datum[projOper.readOneTuple().length];
			readOneTupleFromOper = projOper.readOneTuple();
		}
		
		//Check if the datum[] contains groupby column name
		/*for(Datum itr:readOneTupleFromOper){
			if(groupByColumnReferences.contains(itr.getColumn().getColumnName())){
			isGroupByColumnFlag = true;
			}
		}*/
		
		
		/* logic - for single parameter in groupby column
		 * flag true - only one parameter in agg func;
		 * flag false - list of parameters in agg func 
		 */
		if(isGroupByColumnSingleFlag==true){
			
			//read the single tuple element from the entire tuple and match the group by column name and when it is matched, set "isGroupByTupleMatch = true" 
			for(int i=0;i<readOneTupleFromOper.length;i++){
				Datum singleTupleElement = readOneTupleFromOper[i];
				dLong datumInLong;
				dString datumInString;
				dDate datumInDate;
				//find the type of datum
				if(singleTupleElement instanceof dLong){
					System.out.println("SINGLE READONETUPLE IS DLONG");
					datumInLong = (dLong) singleTupleElement;
		
					if(datumInLong.getColumn().getColumnName().equals(singleParamColumnName)){
						System.out.println("PRINT COLUMN NAME FR SINGLE DATUM: "+datumInLong.getColumn().getColumnName());
						System.out.println("PRINT AGG FUNC PARAM: "+singleParamColumnName);
						matchDatum = datumInLong;
						System.out.println("MATCH DATUM: "+matchDatum);
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
				
			//need to replace the computed datum in the final datum array to be passed over to readOneTuple
			//List<Datum> datumList = Arrays.asList(readOneTupleFromOper);
			//if(datumList.contains(matchDatum)){
				//int matchedDatumIndex = datumList.indexOf(matchDatum);
			//	datumList.set(matchedDatumIndex, aggregateSumDatum);
			//}
			//readOneTupleFromOper = datumList.toArray(readOneTupleFromOper);
			
			
			
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
		
		
	}
	
	private void printTuple(Datum[] row) {
		if(row!=null && row.length !=0) {
		for(Datum col : row) {
			System.out.print(col + "|");
		}
		System.out.println();
		}
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(EqualsTo arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}
//implement the logic for SelectExpressionItem which is being visited by this object
	
}
