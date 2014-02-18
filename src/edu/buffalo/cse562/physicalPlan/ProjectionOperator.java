package edu.buffalo.cse562.physicalPlan;

//import net.sf.jsqlparser.expression.Expression;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.Operator;
import edu.buffalo.cse562.physicalPlan.Tuple;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;

public class ProjectionOperator implements Operator {

	Operator input;

	String temp = null;
	List<SelectExpressionItem> selectcolums;

	

	public ProjectionOperator(Operator input,
			List<SelectExpressionItem> selectcolums) {
		this.selectcolums = selectcolums;
		this.input = input;
		
	}

	
	public void resetStream() {
		input.resetStream();

	}

	public Datum[] readOneTuple() {
		logManager lg = new logManager();
//		map = new LinkedHashMap();
		Datum[] t = null;
		Datum[] tempDatum = null;
		ArrayList<Datum> listDatum;
		t = input.readOneTuple();

		if (t != null) {
			
			
		} else {
			return null;
		}

		
		return tempDatum;

	}
}
