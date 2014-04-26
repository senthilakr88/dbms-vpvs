package edu.buffalo.cse562.physicalPlan;

import edu.buffalo.cse562.structure.Datum;

public class LimitOperator implements Operator{

	Operator oper;
	Long limit;
	
	public LimitOperator (Operator oper, Long limit) {
		this.oper = oper;
		this.limit = limit;
	}
	public void resetStream() {
		oper.resetStream();
		
	}

	@Override
	public Datum[] readOneTuple() {
		if(limit > 0) {
			--limit;
			return oper.readOneTuple();
		} else {
			return null;
		}
		
	}
	@Override
	public void resetTupleMapping() {
		return;		
	}

}
