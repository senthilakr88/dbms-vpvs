package edu.buffalo.cse562.physicalPlan;

import java.util.Date;

import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import edu.buffalo.cse562.sql.expression.evaluator.ColumnFetcher;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/* @author - Vinoth Selvaraju
 * logic - implements sort merge join algorithm that merges two sorted relation (along the join column) with a join key
 */
public class SortMergeJoinOperator implements Operator {
	Operator left; 
	Operator right; 
	Expression expr;
	Column leftJoinKey;
	Column rightJoinKey;
	
	public SortMergeJoinOperator(Operator left, 
			Operator right, Expression expr){
		this.left = left;
		this.right = right;
		this.expr = expr;
		parseJoinExpression();
	}
	
	@Override
	public void resetStream() {
		left.resetStream();
		right.resetStream();
	}
	
	/*	Read one tuple from joined tuple
	 * 	input - Nothing
	 * 	output - Single Datum[] after joining 2 Table
	 */
	@Override
	public Datum[] readOneTuple() {
		//output Datum[]
		Datum[] sortMergeJoinedDatum = null;
		Datum[] inputDatum1 = left.readOneTuple();
		Datum[] inputDatum2 = right.readOneTuple();
		
//		if(inputDatum2 == null || inputDatum1 == null){
//			System.out.println("NULL OCCURED");
//		}
		
//		printTuple(inputDatum1);
//		System.out.println("\n");
//		printTuple(inputDatum2);
//		System.out.println("\n");
		
		//loop until any one relation is completely scanned
		while(inputDatum1!=null && inputDatum2!=null){
			//get the join attribute datum from each file scan operator
			Datum singleDatumValue1 = getDatum(inputDatum1,leftJoinKey);
			Datum singleDatumValue2 = getDatum(inputDatum2,rightJoinKey);
			if(singleDatumValue1!=null && singleDatumValue2!=null){
				//compareDatum return 0 if 2 datum are same
				//System.out.println("INSIDE COMP");
				int compValue = compareDatum(singleDatumValue1,singleDatumValue2);
				if( compValue == 0){
					//System.out.println("TUPLES MATCH");
					sortMergeJoinedDatum = sortMergeJoin(inputDatum1, inputDatum2);
					//read another tuple from the 2 relations
					//inputDatum1 =left.readOneTuple();
					//inputDatum2 = right.readOneTuple();
					break;
				}
				else if(compValue == 1){
					//read another tuple from the relation1
					inputDatum1 =left.readOneTuple();
				}
				else{
					//read another tuple from the relation2
					inputDatum2 = right.readOneTuple();
				}
			}	
		}
		//printTuple(sortMergeJoinedDatum);
		return sortMergeJoinedDatum;
	}
	
	/* return the datum value in Object state
	 * input - Datum
	 * output - Datum value in Object
	 */
	private Object findDatumValue(Datum singleDatum){
		Object datumValue = null;
		if (singleDatum instanceof Datum.dLong) {
			datumValue = ((Datum.dLong) singleDatum).getValue();

		} else if (singleDatum instanceof Datum.dDate) {
			datumValue = ((Datum.dDate) singleDatum).getValue();

		} else if (singleDatum instanceof Datum.dString) {
			datumValue = ((Datum.dString) singleDatum).getValue();

		} else if (singleDatum instanceof Datum.dDecimal) {
			datumValue = ((Datum.dDecimal) singleDatum).getValue();
		}
		return datumValue;
	}
	
	/* Compare 2 datum
	 * input - 2 Datums
	 * output -  0 - if equal
	 * 			-1 - left>right
	 * 			+1 - right>left
	 */
	private int compareDatum(Datum leftValue, Datum rightValue){
		int output=10;
		Object leftDatumValue = findDatumValue(leftValue);
		Object rightDatumValue = findDatumValue(rightValue);
		//perform comparisons after finding the object types as Double/Long/String/Date
		if(leftDatumValue instanceof Double){
			Double left = (Double) leftDatumValue;
			Double right = (Double) rightDatumValue;
			if(left==right){
				output = 0;
			}
			else if(left>right){
				output = -1;
			}
			else{
				output = 1;
			}
			return output;
		}
		else if(leftDatumValue instanceof Long){
			Long left = (Long) leftDatumValue;
			Long right = (Long) rightDatumValue;
			if(left==right){
				output = 0;
			}
			else if(left>right){
				output = -1;
			}
			else{
				output = 1;
			}
			return output;
		}
		else if(leftDatumValue instanceof Date){
			Date left = (Date) leftDatumValue;
			Date right = (Date) rightDatumValue;
			if(left==right){
				output = 0;
			}
			else if(left.after(right)){
				output = -1;
			}
			else{
				output = 1;
			}
			return output;
		}
		else if (leftDatumValue instanceof String){
			String left = (String) leftDatumValue;
			String right = (String) rightDatumValue;
			int compValue = left.compareToIgnoreCase(right);
			
			if(compValue==0){
				output = 0;
			}
			else if(compValue<0){
				output = 1;
			}
			else{
				output = -1;
			}
			return output;
		}
		return output;
	}
	
	/*	get join column datum from Datum[]
	 * 	input - Datum[] & join column specific to the relation (Column)
	 * 	output - Datum
	 */
	private Datum getDatum(Datum[] inputDatum1, Column joinColumn) {
		Datum singleDatumValue = null;
	
		for(int i=0;i<inputDatum1.length;i++){
			if(inputDatum1[i].getColumn().getColumnName().trim().equalsIgnoreCase(joinColumn.getColumnName().trim())){
				singleDatumValue = inputDatum1[i];
				break;
			}
		}
		return singleDatumValue;
	}
	
	/* find join Column in left and right relation and populate the class fields leftJoinKey & rightJoinKey
	 * input - nothing
	 * output - nothing
	 */
	private void parseJoinExpression(){
		//Evaluate the expression to find the join key
		ColumnFetcher cf = new ColumnFetcher();
		expr.accept(cf);
		leftJoinKey = cf.getLeftCol();
		rightJoinKey = cf.getRightCol();
	}
	
	/*	concatenate 2 Datum[]
	 * 	input - 2 Datum[]
	 * 	output - concatenated Datum[]
	 */
	private Datum[] sortMergeJoin(Datum[] inputDatum1, Datum[] inputDatum2) {
		int finalDatumArraySize = inputDatum1.length + inputDatum2.length;
		Datum[] MergedDatumArray = new Datum[finalDatumArraySize];
		System.arraycopy(inputDatum1, 0, MergedDatumArray, 0, inputDatum1.length);
		System.arraycopy(inputDatum2, 0, MergedDatumArray, inputDatum1.length, inputDatum2.length);
		return MergedDatumArray;
	}
	
	/* print tuple
	 * input - Datum[]
	 * output - nothing
	 */
	public void printTuple(Datum[] row) {
		Boolean first = true;
		if (row != null && row.length != 0) {
			for (Datum col : row) {
				if (!first)
					System.out.print("|" + col);
				else {
					System.out.print(col);
					first = false;
				}
			}
		}
	}

	@Override
	public void resetTupleMapping() {
		// TODO Auto-generated method stub
		
	}
}