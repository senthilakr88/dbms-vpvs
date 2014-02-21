package edu.buffalo.cse562.physicalPlan;

import net.sf.jsqlparser.schema.Table;

public class JoinOperator implements Operator {

	Operator left;
	Operator right;
	Datum[] leftTuple;
	Boolean firstEntry;

	public JoinOperator(Operator left, Operator right) {
		this.left = left;
		this.right = right;
		this.firstEntry = true;
	}

	@Override
	public void resetStream() {
		right.resetStream();
	}

	@Override
	public Datum[] readOneTuple() {
		Datum[] lt = null, rt = null;
		Datum[] t = null;
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
				// System.out.println("reading left");
				if (lt == null) {
					return null;
				}
				setTuple(lt);
				right.resetStream();
				rt = right.readOneTuple();
			}
			// System.out.println("left :: "+lt);
			// System.out.println("right :: "+rt);
			t = combine(lt, rt);
			// System.out.println("join:: "+t);
			return t;
		} else
			return null;
	}

	private Datum[] combine(Datum[] lt, Datum[] rt) {
		int i = 0, j = 0;
		Datum[] temp = new Datum[lt.length + rt.length];

		for (i = 0; i < lt.length; i++) {
			temp[i] = lt[i];
//			System.out.println(lt[i].toComString());
		}
		for (j = 0; j < rt.length; j++, i++) {
			temp[i] = rt[j];
//			System.out.println(rt[j].toComString());
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
