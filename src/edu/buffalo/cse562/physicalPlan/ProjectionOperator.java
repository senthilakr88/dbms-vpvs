package edu.buffalo.cse562.physicalPlan;

//import net.sf.jsqlparser.expression.Expression;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.Operator;
import edu.buffalo.cse562.physicalPlan.Tuple;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;

public class ProjectionOperator implements Operator {

	Operator input;

	String temp = null;
	List<SelectExpressionItem> selectcolumns;
	boolean isTupleMapPresent;


	public ProjectionOperator(Operator input,
			List<SelectExpressionItem> selectcolumns) {
		this.selectcolumns = selectcolumns;
		this.input = input;
		this.isTupleMapPresent = true;

	}


	public void resetStream() {
		input.resetStream();

	}

	public Datum[] readOneTuple() {
		logManager lg = new logManager();
		Datum[] t = null;
		Datum[] listDatum = new Datum[selectcolumns.size()];
		
		t = input.readOneTuple();
		if (t != null) {
			int i=0;

			Iterator<SelectExpressionItem> iter=selectcolumns.iterator();
			while(iter.hasNext()){
				SelectExpressionItem newItem = iter.next();
				Expression e = newItem.getExpression();
				if(isTupleMapPresent) {
					TupleStruct.setTupleTableMap(t);
					isTupleMapPresent = false;
				}
				CalcTools calc = new CalcTools(t); 
				e.accept(calc);
				lg.logger.log(Level.INFO, calc.getResult().toString());
				Column newCol = null;
//				Table result = new Table("", "ResultTable");
				if (newItem.getAlias()!=null){
					newCol = new Column(null, newItem.getAlias());
				}
				else {
					newCol = calc.getColumn();
//					newCol = new Column(result, newItem.toString());
				}

				Object ob = calc.getResult();
				Datum tempDatum = null;
				if (ob instanceof Long) {
					lg.logger.log(Level.INFO, "========Long");
					tempDatum = new Datum.dLong(ob.toString(), newCol);
				} else if (ob instanceof String) {
					lg.logger.log(Level.INFO, "========String");
					tempDatum = new Datum.dString((String) ob, newCol);
				} else if (ob instanceof java.util.Date) {
					lg.logger.log(Level.INFO, "=========Date");
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					tempDatum = new Datum.dDate(df.format(ob), newCol);
				} else {
					lg.logger.log(Level.INFO, "Wrong Type");
					try {
						throw new Exception("Not aware of this data type ");
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				lg.logger.log(Level.INFO, tempDatum.toComString());
				listDatum[i]=tempDatum;
				i++;
				

			}
		}
		else {
			return null;
		}

		return listDatum;

	}
}
