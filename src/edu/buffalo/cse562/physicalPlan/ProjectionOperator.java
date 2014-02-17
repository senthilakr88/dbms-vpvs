package edu.buffalo.cse562.physicalPlan;

//import net.sf.jsqlparser.expression.Expression;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.physicalPlan.Operator;
import edu.buffalo.cse562.physicalPlan.Tuple;

public class ProjectionOperator implements Operator {

	Operator input;

	String temp = null;
	List<SelectExpressionItem> selectcolums;
	Map map;

	public ProjectionOperator(Operator input,
			List<SelectExpressionItem> selectcolums) {
		this.selectcolums = selectcolums;
		this.input = input;
	}

	public void resetStream()

	{
		input.resetStream();

	}

	public Tuple readOneTuple() {

		map = new HashMap();
		Tuple t = null;
		t = input.readOneTuple();

		if (t != null) {

			Iterator<SelectExpressionItem> iter = selectcolums.iterator();
			while (iter.hasNext()) {
				SelectExpressionItem temp = iter.next();
				if (t.contains(temp.toString())) {
					map.put(temp, t.valueOf(temp.toString()));
				}
			}

		} else {
			return null;
		}

		return new Tuple(map);

	}
}
