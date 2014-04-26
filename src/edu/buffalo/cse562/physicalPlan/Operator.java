package edu.buffalo.cse562.physicalPlan;

import edu.buffalo.cse562.structure.Datum;

public interface Operator {

	public void resetStream();
	public Datum[] readOneTuple();
	public void resetTupleMapping();
}
