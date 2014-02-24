package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

public class JoinOperator implements Operator {

	Operator left;
	Operator right;
	Datum[] leftTuple;
	Boolean firstEntry;
	Expression expr;
	boolean isTupleMapPresent;
	List<Datum[]> buffer;
	Integer bufferMaxSize;
	Integer bufferPointer;
	Boolean isEnd;

	public JoinOperator(Operator left, Operator right, Expression expression) {
		this.left = left;
		this.right = right;
		this.firstEntry = true;
		this.expr = expression;
		this.isTupleMapPresent = true;
		this.bufferMaxSize = 10000;
		this.bufferPointer = -1;
		this.buffer = new ArrayList<Datum[]>(bufferMaxSize);
		this.isEnd = false;
	}

	@Override
	public void resetStream() {
		right.resetStream();
	}

	public Datum[] readOneTuple() {

		Datum[] lt = null, rt = null;
		Datum[] t = null;
//		System.out.println("Inside :: " +bufferPointer + " :: " + buffer.size() + " :: " + bufferPointer.compareTo(buffer.size()));
		if (bufferPointer.compareTo(buffer.size()-1) != 0 && buffer.size() <= bufferMaxSize) {
//			System.out.println(bufferPointer);
			++bufferPointer;
			return buffer.get(bufferPointer);
		} else {
			
			bufferPointer = -1;
			buffer = new ArrayList<Datum[]>(bufferMaxSize);
			while (buffer.size() < bufferMaxSize) {
				// long startTime = System.currentTimeMillis();
//				System.out.println("Filling buffer :: " + buffer.size());
				if (firstEntry) {
					lt = left.readOneTuple();
					setTuple(lt);
					// System.out.println("entry");
					firstEntry = false;
				}
				lt = getTuple();
				if (lt != null) {
					rt = right.readOneTuple();
					if (rt == null) {
						lt = left.readOneTuple();

						if (lt == null) {
							isEnd = true;
							break;
						}
						setTuple(lt);
						right.resetStream();
						rt = right.readOneTuple();
					}
					// On expression evaluation
					t = combine(lt, rt);

					if (t == null) {
						return null;
					}
					if (!evaluate(t, expr)) {
						t = null;
					}

					if (t != null) {
						buffer.add(t);
					}

				} else {
					return null;
				}
			}
		}
//		System.out.println("Outside :: " +bufferPointer + " :: " + buffer.size() + " :: " + bufferPointer.compareTo(buffer.size()));
		if(!isEnd) {
			++bufferPointer;
			return buffer.get(bufferPointer);
		} else {
			return null;
		}
	}

	private Datum[] tupleBufFull() {
		++bufferPointer;
		return buffer.get(bufferPointer);
	}

	private boolean evaluate(Datum[] t, Expression expr2) {
		if (expr2 != null) {

			if (isTupleMapPresent) {
				TupleStruct.setTupleTableMap(t);
				isTupleMapPresent = false;
			}
			CalcTools calc = new CalcTools(t);
			expr.accept(calc);
			// System.out.println(calc.getAccumulatorBoolean());
			return calc.getAccumulatorBoolean();
		} else {
			return true;
		}
	}

	private Datum[] combine(Datum[] lt, Datum[] rt) {
		int i = 0, j = 0;
		Datum[] temp = new Datum[lt.length + rt.length];

		for (i = 0; i < lt.length; i++) {
			temp[i] = lt[i];
			// System.out.println(lt[i].toComString());
		}
		for (j = 0; j < rt.length; j++, i++) {
			temp[i] = rt[j];
			// System.out.println(rt[j].toComString());
		}
		return temp;
	}

	public Datum[] getTuple() {
		return leftTuple;
	}

	public void setTuple(Datum[] lt) {
		this.leftTuple = lt;
	}

}
