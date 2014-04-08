package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
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
	int leftIndex;
	int rightIndex;
	Boolean initialDatumReadflag = false;
	ArrayList<Datum[]> tempMatchTuple = new ArrayList<Datum[]>();
	ArrayList<Datum[]> inputDatum2TempList = new ArrayList<Datum[]>();
	Boolean matchFlag = false;
	Datum[] inputDatum1 = null;
	Datum[] inputDatum2 = null;
	Boolean checkListFlag = false;
	Boolean noUsualFlag = false;
	
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
		//System.out.println("\n-----------------------------");
		//output Datum[]
		Datum[] sortMergeJoinedDatum = null;
		if(initialDatumReadflag == false){
			//System.out.println("Am I printing just once?");
			inputDatum1 = left.readOneTuple();
			inputDatum2 = right.readOneTuple();
			leftIndex = getDatumIndex(inputDatum1,leftJoinKey);
			rightIndex = getDatumIndex(inputDatum2,rightJoinKey);
			System.out.println(rightIndex);
			initialDatumReadflag = true;
		}
		else{
			if(matchFlag = true){
//				keep the left same and vary the right
				Datum leftSingleDatum = null;
				Datum rightSingleDatum = null;
				if(inputDatum2!=null){
					inputDatum2 =right.readOneTuple();
					inputDatum2TempList.add(inputDatum2);
					//Datum[] inputDatum2Temp = inputDatum2;
					leftSingleDatum = inputDatum1[leftIndex];
					if(inputDatum2!=null){
						rightSingleDatum = inputDatum2[rightIndex];
					}
				}
				while(inputDatum2!=null && compareDatum(leftSingleDatum,rightSingleDatum) == 0){
					//System.out.println("first join order");
					checkListFlag = true;
					//System.out.println(matchFlag);
					//printTuple(inputDatum1);
					//printTuple(inputDatum2);
					sortMergeJoinedDatum = sortMergeJoin(inputDatum1, inputDatum2);
					//System.out.println("------------------------END--------------------------");
					return sortMergeJoinedDatum;
					//break;
//					inputDatum2 = right.readOneTuple();
//					rightSingleDatum = inputDatum2[rightIndex];
//					inputDatum2TempList.add(inputDatum2);
				}
			}
			if(matchFlag = true && checkListFlag == true){
				checkListFlag = false;
				matchFlag = false;
				inputDatum1 = left.readOneTuple();
				Datum leftSingleDatum = inputDatum1[leftIndex];
				if(inputDatum2TempList.size()>0){
					for(Datum[] passedInputDatum2:inputDatum2TempList){
						Datum rightSingleDatum=passedInputDatum2[rightIndex];
						while(inputDatum1!=null && compareDatum(leftSingleDatum,rightSingleDatum) == 0){
							//printTuple(inputDatum1);
							//printTuple(inputDatum2);
							//System.out.println("second join order");
							sortMergeJoinedDatum = sortMergeJoin(inputDatum1, passedInputDatum2);
							break;
//								inputDatum1 = left.readOneTuple();
//								leftSingleDatum = inputDatum1[leftIndex];
						}
					}
				}
			}	
		}
		
		
		//loop until any one relation is completely scanned
		while(inputDatum1!=null && inputDatum2!=null){
			//System.out.println("INSIDE WHILE");
			//get the join attribute datum from each file scan operator
//			Datum singleDatumValue1 = getDatum(inputDatum1,leftJoinKey);
//			Datum singleDatumValue2 = getDatum(inputDatum2,rightJoinKey);
			
			Datum singleDatumValue1 = inputDatum1[leftIndex];
			Datum singleDatumValue2 = inputDatum2[rightIndex];
			
			//if(singleDatumValue1!=null && singleDatumValue2!=null){
				//compareDatum return 0 if 2 datum are same
				//System.out.println("INSIDE COMP");
				int compValue = 10;
				compValue = compareDatum(singleDatumValue1,singleDatumValue2);
				//System.out.println("\nPRINT COMP VALUE"+ compValue);
				if( compValue == 0){
					//System.out.println("TUPLES MATCH");
					//System.out.println("MATCHED LEFT TUPLE");
					//System.out.println("Usual merge");
					//printTuple(inputDatum1);
					//System.out.println("MATCHED RIGHT TUPLE");
					//printTuple(inputDatum2);
					
					matchFlag=true;
					sortMergeJoinedDatum = sortMergeJoin(inputDatum1, inputDatum2);
					break;
				}
				else if(compValue < 0){
//					if(tempMatchTuple.size()>1){
//						tempMatchTuple.clear();
//					}
					//read another tuple from the relation1
					inputDatum1 =left.readOneTuple();
					//System.out.println("INPUT TUPLE FROM LEFT");
					//printTuple(inputDatum1);
				}
				else{
//					if(tempMatchTuple.size()>1){
//						tempMatchTuple.clear();
//					}
					//read another tuple from the relation2
					inputDatum2 = right.readOneTuple();
					//System.out.println("INPUT TUPLE FROM RIGHT");
					//printTuple(inputDatum2);
				}
			}	
	//	}
		//printTuple(sortMergeJoinedDatum);
		//System.out.println("------------------------END--------------------------");
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
	private Integer compareDatum(Datum leftValue, Datum rightValue){
		Object leftDatumValue = findDatumValue(leftValue);
		Object rightDatumValue = findDatumValue(rightValue);
		//perform comparisons after finding the object types as Double/Long/String/Date
		if(leftDatumValue instanceof Double){
			Double left = (Double) leftDatumValue;
			Double right = (Double) rightDatumValue;
//			if(left==right){
//				output = 0;
//			}
//			else if(left>right){
//				output = -1;
//			}
//			else{
//				output = 1;
//			}
//			return output;
			return left.compareTo(right);
		}
		else if(leftDatumValue instanceof Long){
			Long left = (Long) leftDatumValue;
			Long right = (Long) rightDatumValue;
			
			
//			if(left==right){
//				output = 0;
//			}
//			else if(left>right){
//				output = -1;
//			}
//			else{
//				output = 1;
//			}
			return left.compareTo(right);
		}
		else if(leftDatumValue instanceof Date){
			Date left = (Date) leftDatumValue;
			Date right = (Date) rightDatumValue;
//			if(left==right){
//				output = 0;
//			}
//			else if(left.after(right)){
//				output = -1;
//			}
//			else{
//				output = 1;
//			}
			return left.compareTo(right);
		}
		else if (leftDatumValue instanceof String){
			String left = (String) leftDatumValue;
			String right = (String) rightDatumValue;
//			int compValue = left.compareToIgnoreCase(right);
//			
//			if(compValue==0){
//				output = 0;
//			}
//			else if(compValue<0){
//				output = 1;
//			}
//			else{
//				output = -1;
//			}
			return left.compareToIgnoreCase(right);
		}
		else{
			System.out.println("Error: Tuple not instance of Long/Double/Date/String");
			return null;
		}
		
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
	
	/*	get join column datum index from Datum[]
	 * 	input - Datum[] & join column specific to the relation (Column)
	 * 	output - Datum
	 */
	private int getDatumIndex(Datum[] inputDatum1, Column joinColumn) {
		int index = -1;
	
		for(int i=0;i<inputDatum1.length;i++){
			if(inputDatum1[i].getColumn().getColumnName().trim().equalsIgnoreCase(joinColumn.getColumnName().trim())){
				index = i;
				break;
			}
		}
		return index;
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
			System.out.println();
		}
	}

	@Override
	public void resetTupleMapping() {
		// TODO Auto-generated method stub
		
	}
}