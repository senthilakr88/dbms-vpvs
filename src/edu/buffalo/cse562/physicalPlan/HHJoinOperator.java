package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import edu.buffalo.cse562.sql.expression.evaluator.ColumnFetcher;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class HHJoinOperator implements Operator {

	Operator left;
	Operator right;
	Column leftKey;
	Column rightKey;
	int leftColIndex;
	int rightColIndex;
	Expression expr;
	Integer partitions;
	boolean firstTime;
	boolean rightIndexRet;
	Integer arrayPointer;
	Map<Integer, ArrayList<Datum[]>> hashTable;
	Datum[] rightDatum;

	public HHJoinOperator(Operator left, Operator right, Expression expression) {
		this.left = left;
		this.right = right;
		this.expr = expression;
		this.partitions = 1000;
		firstTime = true;
		rightIndexRet = true;
		arrayPointer = 0;
		hashTable = new HashMap<Integer, ArrayList<Datum[]>>();
		buildHashTable(left);
	}

	public void resetStream() {
		right.resetStream();
	}

	public Datum[] readOneTuple() {
		Datum[] joinTuple = null;
		ArrayList<Datum[]> listTuple;
		Object key;
		Integer keyNo;
		Datum[] leftDatum;
		if (arrayPointer == 0) {
			rightDatum = right.readOneTuple();
			if (rightIndexRet) {
				TupleStruct.setTupleTableMap(rightDatum);
				rightColIndex = TupleStruct.getColIndex(rightDatum, rightKey);
				rightIndexRet = false;
			}
			if(rightDatum == null) {
				return null;
			}
		}
		key = TupleStruct.getKey(rightDatum, rightColIndex);
		keyNo = partNo(key);
		listTuple = hashTable.get(keyNo);
		leftDatum = searchHashTable(listTuple, key);
		if(leftDatum == null) {
			rightDatum = right.readOneTuple();
			if(rightDatum == null) {
				return null;
			}
			key = TupleStruct.getKey(rightDatum, rightColIndex);
			keyNo = partNo(key);
			listTuple = hashTable.get(keyNo);
			leftDatum = searchHashTable(listTuple, key);
			
		}
		joinTuple = combine(leftDatum, rightDatum);
		return joinTuple;

	}

	public Datum[] searchHashTable(ArrayList<Datum[]> listTuple, Object rightKey) {
		Datum[] leftDatum;
		int compareTo;
		Object leftKey;
		int size = listTuple.size();
		while (arrayPointer != size) {
			leftDatum = listTuple.get(arrayPointer);
			leftKey = TupleStruct.getKey(leftDatum, leftColIndex);
			compareTo = TupleStruct.compare(leftKey, rightKey);
			++arrayPointer;
			if (compareTo == 0) {				
				return leftDatum;
			} 
		}
		arrayPointer = 0;
		return null;
	}

	

	public void buildHashTable(Operator oper) {
		Object key;
		int keyNo;
		ArrayList<Datum[]> listTuple;
		Datum[] tempTuple = oper.readOneTuple();

		while (tempTuple != null) {
			if (firstTime) {
				parseExpression();
				TupleStruct.setTupleTableMap(tempTuple);
				leftColIndex = TupleStruct.getColIndex(tempTuple, leftKey);
				firstTime = false;
			}
			key = TupleStruct.getKey(tempTuple, leftColIndex);
			keyNo = partNo(key);
			if (!hashTable.containsKey(keyNo)) {
				listTuple = new ArrayList<Datum[]>();
				listTuple.add(tempTuple);
				hashTable.put(keyNo, listTuple);
			} else {
				listTuple = hashTable.get(keyNo);
				listTuple.add(tempTuple);
				hashTable.put(keyNo, listTuple);
			}
			tempTuple = oper.readOneTuple();
		}
		//printTuple(hashTable);
	}

	public void parseExpression() {
		ColumnFetcher cf = new ColumnFetcher();
		expr.accept(cf);
		leftKey = cf.getLeftCol();
		rightKey = cf.getRightCol();
	}

	public int partNo(Object key) {
		return hashFunc(key) % partitions;
	}

	public int hashFunc(Object key) {
		return Math.abs(key.hashCode());
	}

	public void printTuple(Map<Integer, ArrayList<Datum[]>> hashTable) {
		Iterator it = hashTable.entrySet().iterator();
		ArrayList<Datum[]> tempList;
	    while (it.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it.next();
	        System.out.print(pairs.getKey() + " = ");
	        tempList = (ArrayList<Datum[]>) pairs.getValue();
	        Iterator it1 = tempList.iterator();
	        while(it1.hasNext()) {
	        	printTuple((Datum[])it1.next());
	        	System.out.print(",");
	        }
	        System.out.println();
	    }
	}
	
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
			// System.out.println();
		}
	}

	private boolean evaluate(Datum[] t, Expression expr2) {
		if (expr2 != null) {

			CalcTools calc = new CalcTools(t);
			expr.accept(calc);
			// //System.out.println(calc.getAccumulatorBoolean());
			return calc.getAccumulatorBoolean();
		} else {
			return true;
		}
	}

	private Datum[] combine(Datum[] lt, Datum[] rt) {
		int i = 0, j = 0;
		int len = 0;
		len += lt.length;
		len += rt.length;
		Datum[] temp = new Datum[len];

		for (i = 0; i < lt.length; i++) {
			temp[i] = lt[i];
			// //System.out.println(lt[i].toComString());
		}
		for (j = 0; j < rt.length; j++, i++) {
			temp[i] = rt[j];
			// //System.out.println(rt[j].toComString());
		}
		// System.out.println("Combining :: " + lt[0] + " :: " +rt[0]);
		return temp;
	}
}
