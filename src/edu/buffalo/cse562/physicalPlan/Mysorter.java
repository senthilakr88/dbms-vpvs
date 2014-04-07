package edu.buffalo.cse562.physicalPlan;

import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;

public class Mysorter implements Comparator<Datum[]> {

	List<OrderByElement> ordEle;
	Boolean isTupleMapPresent;

	public Mysorter(List<OrderByElement> elements) {
		this.ordEle = elements;
		isTupleMapPresent = true;
	}

	@Override
	public int compare(Datum[] t1, Datum[] t2) {

		Iterator iter = ordEle.iterator();
		CalcTools calc1 = null, calc2 = null;
		int comparison = -2;
		while (iter.hasNext()) {

			OrderByElement ele = (OrderByElement) iter.next();
			Expression exe = ele.getExpression();
			if(isTupleMapPresent) {
				TupleStruct.setTupleTableMap(t1);
//				System.out.println(TupleStruct.getTupleTableMap());
				if(!TupleStruct.isNestedCondition())
					isTupleMapPresent = false;
			}
			
//			printTuple(t1);
			
			calc1 = new CalcTools(t1);
			exe.accept(calc1);
			calc2 = new CalcTools(t2);
			exe.accept(calc2);
			
			comparison = TupleStruct.getCompareValue(calc1.getResult(), calc2.getResult(),
					ele.isAsc());
			
			if (comparison != 0) {
				//System.out.println("In :: " + calc1.getResult() + " : " + calc2.getResult() + " : " + comparison);
				return comparison;
			}
		}
		//System.out.println("Out :: " + calc1.getResult() + " : " + calc2.getResult() + " : " + comparison);
		return comparison;
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

}

