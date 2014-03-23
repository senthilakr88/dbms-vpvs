package edu.buffalo.cse562.physicalPlan;

import java.util.Date;
import net.sf.jsqlparser.schema.Column;

public class SortMergeJoinOperator implements Operator {
	FileScanOperator fileScanOperRelation1; 
	FileScanOperator fileScanOperRelation2; 
	Column joinKey;
	
	public SortMergeJoinOperator(FileScanOperator fileScanOperRelation1, 
			FileScanOperator fileScanOperRelation2, Column joinKey){
		this.fileScanOperRelation1 = fileScanOperRelation1;
		this.fileScanOperRelation2 = fileScanOperRelation2;
		this.joinKey = joinKey;
		
	}
	
	@Override
	public void resetStream() {
		// TODO Auto-generated method stub
		
	}
	
	/*	Read one tuple from joined tuple
	 * 	input - Nothing
	 * 	output - Single Datum[] after joining 2 Table
	 */
	@Override
	public Datum[] readOneTuple() {
		//output Datum[]
		Datum[] sortMergeJoinedDatum = null;
		Datum[] inputDatum1 = fileScanOperRelation1.readOneTuple();
		Datum[] inputDatum2 = fileScanOperRelation2.readOneTuple();
		
		//loop until any one relation is completely scanned
		while(inputDatum1!=null && inputDatum2!=null){
			
			//get the join attribute datum from each file scan operator
			Datum singleDatumValue1 = getDatum(inputDatum1);
			Datum singleDatumValue2 = getDatum(inputDatum2);
			if(singleDatumValue1!=null && singleDatumValue2!=null){
				
				//compareDatum return 0 if 2 datum are same
				int compValue = compareDatum(singleDatumValue1,singleDatumValue2);
				if( compValue == 0){
					sortMergeJoinedDatum = sortMergeJoin(inputDatum1, inputDatum2);
					//read another tuple from the 2 relations
					inputDatum1 =fileScanOperRelation1.readOneTuple();
					inputDatum2 = fileScanOperRelation2.readOneTuple();
				}
				else if(compValue == 1){
					//read another tuple from the relation1
					inputDatum1 =fileScanOperRelation1.readOneTuple();
				}
				else{
					//read another tuple from the relation2
					inputDatum2 = fileScanOperRelation2.readOneTuple();
				}
			}	
		}
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
	
	/*	get join key datum from Datum[]
	 * 	input - Datum[]
	 * 	output - Datum
	 */
	private Datum getDatum(Datum[] inputDatum1) {
		Datum singleDatumValue = null;
		for(int i=0;i<inputDatum1.length;i++){
			if(inputDatum1[i].getColumn().getColumnName().trim().equalsIgnoreCase(joinKey.getColumnName().trim())){
				singleDatumValue = inputDatum1[i];
				break;
			}
		}
		return singleDatumValue;
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
}
