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
	String rightTable;

	public HHJoinOperator(Operator left, Operator right, Expression expression, String rightTable) {
		this.left = left;
		this.right = right;
		this.expr = expression;
		this.partitions = 1000;
		this.rightTable = rightTable;
		firstTime = true;
		rightIndexRet = true;
		arrayPointer = 0;
		hashTable = new HashMap<Integer, ArrayList<Datum[]>>();
//		System.out.println(leftColIndex + " :: " + rightColIndex);
//		System.out.println("Hash Table Construct starting ");
		buildHashTable();
//		printTuple(hashTable);
	}

	public void resetStream() {
		right.resetStream();
	}

	public Datum[] readOneTuple() {
//		System.out.println("entering readOneTuple");
		Datum[] joinTuple = null;
		ArrayList<Datum[]> listTuple;
		Object key;
		Integer keyNo;
		Datum[] leftDatum = null;
		if (arrayPointer == 0) {
			right.resetStream();
			rightDatum = right.readOneTuple();
			if (rightIndexRet) {
				TupleStruct.setTupleTableMap(rightDatum);
//				System.out.println("Computing right index");
				rightColIndex = TupleStruct.getColIndex(rightDatum, rightKey);
				rightIndexRet = false;
			}
			if (rightDatum == null) {
				return null;
			}
		}

		key = TupleStruct.getKey(rightDatum, rightColIndex);
		keyNo = partNo(key);
		listTuple = hashTable.get(keyNo);
//		System.out.println("Validating listTuple");

		if (listTuple != null) {
//			System.out.println("Finding FirsTuple");
//			System.out.println(key + " :: " + keyNo);
			leftDatum = searchHashTable(listTuple, key);
		}

		while (leftDatum == null) {
			arrayPointer = 0;
			right.resetTupleMapping();
			rightDatum = right.readOneTuple();
			if (rightDatum == null) {
				return null;
			}
			key = TupleStruct.getKey(rightDatum, rightColIndex);
			keyNo = partNo(key);
			listTuple = hashTable.get(keyNo);
			if (listTuple == null) {
//				System.out.println("listTuple is empty");
				while (listTuple == null) {
					right.resetTupleMapping();
					rightDatum = right.readOneTuple();
					if (rightDatum == null) {
						return null;
					}
					key = TupleStruct.getKey(rightDatum, rightColIndex);
					keyNo = partNo(key);
					listTuple = hashTable.get(keyNo);
				}
//				System.out.println("non traversal :: finding a datum after listTraversal" );
				printTuple(leftDatum);
				leftDatum = searchHashTable(listTuple, key);
			} else {
//				System.out.println(key + " :: " + keyNo);
//				System.out.println("non - list Traversal" );
				printTuple(leftDatum);
				leftDatum = searchHashTable(listTuple, key);
			}

		}

//		System.out.println("--------------------");
//		printTuple(leftDatum);
//		System.out.println();
//		printTuple(rightDatum);
//		System.out.println();
//		System.out.println("--------------------");
		joinTuple = combine(leftDatum, rightDatum);
//		System.out.print("JoinTuple :: ");
//		printTuple(joinTuple);
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

	public void buildHashTable() {
		Object key;
		int keyNo;
		ArrayList<Datum[]> listTuple;
		Datum[] tempTuple = left.readOneTuple();

//		if(oper == null) {
//			System.out.println("oper is empty");
//		}
//		
//		if(tempTuple == null) {
//			System.out.println("No more Tuples");
//		}
		
//		System.out.println("Table Constructed begins");
		while (tempTuple != null) {
			if (firstTime) {
				
				parseExpression(rightTable);				
//				 System.out.println("Structure :: " + TupleStruct.getTupleTableMap());
//				 System.out.println("LeftKey :: "+leftKey.getColumnName());
//				System.out.println("Right Table parsing completes");
//				System.out.println("Calling Hash Join Column Index");
				TupleStruct.setTupleTableMap(tempTuple);
				leftColIndex = TupleStruct.getColIndex(tempTuple, leftKey);
				firstTime = false;
			}
			key = TupleStruct.getKey(tempTuple, leftColIndex);
			keyNo = partNo(key);
//			System.out.println("Entered to add Hashtable");
			if (!hashTable.containsKey(keyNo)) {
				listTuple = new ArrayList<Datum[]>();
				listTuple.add(tempTuple);
				hashTable.put(keyNo, listTuple);
			} else {
				listTuple = hashTable.get(keyNo);
				listTuple.add(tempTuple);
				hashTable.put(keyNo, listTuple);
			}
			tempTuple = left.readOneTuple();
		}
		
//		System.out.println("Building hash Table completed");
		// printTuple(hashTable);
	}

	public void parseExpression(String rightTable) {
		ColumnFetcher cf = new ColumnFetcher(rightTable);
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
			Map.Entry pairs = (Map.Entry) it.next();
			System.out.print(pairs.getKey() + " = ");
			tempList = (ArrayList<Datum[]>) pairs.getValue();
			Iterator it1 = tempList.iterator();
			while (it1.hasNext()) {
				printTuple((Datum[]) it1.next());
				System.out.println(",");
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
			 System.out.println();
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

	@Override
	public void resetTupleMapping() {
		// TODO Auto-generated method stub

	}
}
