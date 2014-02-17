package edu.buffalo.cse562.physicalPlan;

//import net.sf.jsqlparser.expression.Expression;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.Operator;
import edu.buffalo.cse562.physicalPlan.Tuple;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;

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
		logManager lg = new logManager();
		map = new LinkedHashMap();
		Tuple t = null;
		t = input.readOneTuple();

		if (t != null) {

			Iterator<SelectExpressionItem> iter = selectcolums.iterator();
			while (iter.hasNext()) {
				SelectExpressionItem temp = iter.next();
				
				if (t.contains(temp.toString())) {
					map.put(temp, t.valueOf(temp.toString()));
				} else {
					Expression e = temp.getExpression();
					CalcTools calc = new CalcTools(t);
					e.accept(calc);
					
					map.put(temp.getAlias(), calc.getResult());
//					lg.logger.log(Level.INFO, temp.getAlias().toString());
					//lg.logger.log(Level.INFO, calc.getResult().toString());
				}
				lg.logger.log(Level.INFO, map.keySet().toString());

			}

		} else {
			return null;
		}

		return new Tuple(map);

	}
}
