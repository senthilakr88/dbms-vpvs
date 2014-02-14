package edu.buffalo.cse562.physicalPlan;

import net.sf.jsqlparser.expression.Expression;

public class SelectionOperator implements Operator {
	
	Operator input;
	Expression condition;
	
	public SelectionOperator(Operator input, Expression condition) {
		this.input = input;
		this.condition = condition;
	}
	
	@Override
	public void resetStream() {
		input.resetStream();

	}

	@Override
	public Tuple readOneTuple() {
		Tuple t = null;
		do {
			t = input.readOneTuple();
			if(!evaluate(t,condition)) {
				t = null;
			}
		} while(t==null);
		return t;
	}

	private boolean evaluate(Tuple t, Expression condition2) {
		// TODO Auto-generated method stub
		return false;
	}

}
