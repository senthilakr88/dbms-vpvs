package edu.buffalo.cse562.physicalPlan;

import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import edu.buffalo.cse562.structure.Datum;
import net.sf.jsqlparser.expression.Expression;


public class NLJoinOperator implements Operator {

	Operator left;
	Operator right;
	Datum[] leftTuple;
	Boolean firstEntry;
	Expression expr;
	boolean isTupleMapPresent;
	
	public NLJoinOperator(Operator left, Operator right, Expression expression) {
		this.left = left;
		this.right = right;
		this.firstEntry = true;
		this.expr = expression;
		this.isTupleMapPresent = true;
	}

	@Override
	public void resetStream() {
		right.resetStream();
	}

	public Datum[] readOneTuple() {
		Datum[] lt = null, rt = null;
		Datum[] t = null;

		do {
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
						//System.out.println("reading left :: ");
						return null;
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
					// System.out.println("Condition not Satisfied");
					t = null;
				} // else {
					// System.out.println("Condition satisfied");
				// }
			} else {
				return null;
			}
		} while (t == null);
		// System.out.println("Came here 2");
		// System.out.println("///////////return tuple length"+t.length);
		return t;

	}

	private boolean evaluate(Datum[] t, Expression expr2) {
		if (expr2 != null) {
			
			if(isTupleMapPresent) {
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

	@Override
	public void resetTupleMapping() {
		// TODO Auto-generated method stub
		
	}

}
