package edu.buffalo.cse562.physicalPlan;

import net.sf.jsqlparser.schema.Table;

public class JoinOperator implements Operator {

	Operator left;
	Operator right;
	Tuple leftTuple;
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
	public Tuple readOneTuple() {
		Tuple lt = null, rt = null;
		Tuple t = null;
		if(firstEntry) {
			lt = left.readOneTuple();
			setTuple(lt);
			System.out.println("entry");
			firstEntry = false;
		}
		lt = getTuple();
		if(lt !=null) {
			rt = right.readOneTuple();
			if(rt == null){
				lt = left.readOneTuple();
				System.out.println("reading left");
				if(lt == null) {
					return null;
				}
				setTuple(lt);
				right.resetStream();
				rt = right.readOneTuple();
			}
			System.out.println("left :: "+lt);
			System.out.println("right :: "+rt);
			t = lt.combine(rt);
			System.out.println("join:: "+t);
			return t;
		} else
			return null;
	}
	
	public Tuple getTuple() {
		return leftTuple;
	}
	
	public void setTuple(Tuple leftTuple) {
		this.leftTuple = leftTuple;
	}

}
