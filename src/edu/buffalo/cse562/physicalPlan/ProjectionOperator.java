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



	public ProjectionOperator(Operator input,
			List<SelectExpressionItem> selectcolumns) {
		this.selectcolumns = selectcolumns;
		this.input = input;

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
			lg.logger.log(Level.INFO, "Came into the PROJECTION read one tuple");
			lg.logger.log(Level.INFO, t.toString());
			//		for(int index = 0;index < t.length;index++) {
			//			Datum row = (Datum) t[index];
			//			lg.logger.log(Level.INFO, row.toComString());
			//			
			//		}
			//		lg.logger.log(Level.INFO, t[0].toComString());
			//		lg.logger.log(Level.INFO, t[1].toComString());
			lg.logger.log(Level.INFO, selectcolumns.toString());
			lg.logger.log(Level.INFO, selectcolumns.getClass().getName());
			int i=0;
			
			
			Iterator<SelectExpressionItem> iter=selectcolumns.iterator();
			while(iter.hasNext()){
				SelectExpressionItem newItem = iter.next();
				//				lg.logger.log(Level.INFO, newItem.toString());
				//				if()
				//				lg.logger.log(Level.INFO, newItem.getExpression().toString());
				Expression e = newItem.getExpression();
				CalcTools calc = new CalcTools(t); 
				e.accept(calc);
				lg.logger.log(Level.INFO, calc.getResult().toString());
				Column newCol = null;
				Table result = new Table("ResulSchema", "ResultTable");
				//				String alias = row.getColumn().getTable().getAlias();
				//				String datumColumn = row.getColumn().getColumnName();
				//				if(alias!=null) {
				//					datumColumn = alias +"."+datumColumn;
				//				}
				if (newItem.getAlias()!=null){
					lg.logger.log(Level.INFO, "------------>"+newItem.getAlias().toString());
					//					tempDatum = new Datum(calc.getResult().toString(), newItem.getAlias());
					newCol = new Column(result, newItem.getAlias());
				}
				else {
					lg.logger.log(Level.INFO, "+++++++++++++>"+newItem.toString());
					//					tempDatum = new Datum(calc.getResult().toString(), newItem);
					newCol = new Column(result, newItem.toString());
				}

				Object ob = calc.getResult();
				Datum tempDatum = null;
//				System.out.println(ob.toString());
				if (ob instanceof Long) {
					// tupleKeyValueMap.put(key, Integer.parseInt(value));
					lg.logger.log(Level.INFO, "========Long");
					tempDatum = new Datum.dLong(ob.toString(), newCol);
					//					System.out.print(t[i].toComString());
				} else if (ob instanceof String) {
					// tupleKeyValueMap.put(key, value);
					lg.logger.log(Level.INFO, "========String");
					tempDatum = new Datum.dString((String) ob, newCol);
					//System.out.print(t[i].toComString());
				} else if (ob instanceof java.util.Date) {
					// tupleKeyValueMap.put(key, (new SimpleDateFormat(
					// "YYYY-MM-DD", Locale.ENGLISH).parse(value)));
					lg.logger.log(Level.INFO, "=========Date");
					DateFormat df = new SimpleDateFormat("YYYY-MM-DD");
					tempDatum = new Datum.dDate(df.format(ob), newCol);
//					System.out.println(tempDatum.toComString());
					//System.out.print(t[i].toComString());
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
		//			
		//			int i=0,j=0; 
		//			while(i<t.length)
		//			{
		//				if(selectcolumns.contains(t[i].getColumn()))
		//				{
		//					tempDatum[j]=t[i];
		//					j++;
		//				}
		//
		//			}
		//
		//		} else {
		//			return null;
		//		}


		return listDatum;

	}
}
