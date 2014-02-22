package edu.buffalo.cse562.physicalPlan;

import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.physicalPlan.Datum.dLong;
import edu.buffalo.cse562.physicalPlan.Datum.dString;
import edu.buffalo.cse562.physicalPlan.Datum.dDate;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;

public class Mysorter implements Comparator<Datum[]> {

	List<OrderByElement> ordEle;

	public Mysorter(List<OrderByElement> elements) {
		this.ordEle = elements;
	}

	@Override
	public int compare(Datum[] t1, Datum[] t2) {

		Iterator iter = ordEle.iterator();
		int comparison = -2;
		while (iter.hasNext()) {

			OrderByElement ele = (OrderByElement) iter.next();
			Expression exe = ele.getExpression();
			//System.out.println(exe.toString() + " : " + ele.isAsc());
			CalcTools calc1 = new CalcTools(t1);
			ele.accept(calc1);
			CalcTools calc2 = new CalcTools(t2);
			ele.accept(calc2);
			System.out.println(calc1.getResult() + ":" + calc2.getResult());
			comparison = getCompareValue(calc1.getResult(), calc2.getResult(),
					ele.isAsc());
			if (comparison != 0)
				return comparison;
		}
		return comparison;
	}

	public int getCompareValue(Object t1, Object t2, boolean asc) {
		if (t1 instanceof dLong) {
			Long value1 = (Long)t1;
			Long value2 = (Long)t1;
			int comp = value1.compareTo(value2);
			if(comp == 0) {
				return comp;
			} else if(asc) {
				return comp;
			} else {
				if(comp == -1)
					return 1;
				else 
					return -1;
			}

		} else if (t1 instanceof dString) {
			String value1 = (String)t1;
			String value2 = (String)t1;
			int comp = value1.compareTo(value2);
			if(comp == 0) {
				return comp;
			} else if(asc) {
				return comp;
			} else {
				if(comp == -1)
					return 1;
				else 
					return -1;
			}

		}  else if (t1 instanceof dDate) {
			Date value1 = (Date)t1;
			Date value2 = (Date)t1;
			int comp = value1.compareTo(value2);
			if(comp == 0) {
				return comp;
			} else if(asc) {
				return comp;
			} else {
				if(comp == -1)
					return 1;
				else 
					return -1;
			}
		} else {
			return -2;
		}
		
		
	}
}

