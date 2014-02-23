package edu.buffalo.cse562.physicalPlan;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.buffalo.cse562.physicalPlan.Datum.dDate;
import edu.buffalo.cse562.physicalPlan.Datum.dDecimal;
import edu.buffalo.cse562.physicalPlan.Datum.dLong;
import edu.buffalo.cse562.physicalPlan.Datum.dString;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupbyOperator {
	Operator oper;
	ArrayList<SelectExpressionItem> selectExpressionList;
	List<Column> groupbyList;
	Map<String, Datum[]> groupByMap = new HashMap<String, Datum[]>();
	boolean isTupleMapPresent;
	
	

	public GroupbyOperator(Operator oper,
			ArrayList<SelectExpressionItem> selectExpressionList, 
			List<Column> groupbyList) {
		this.oper = oper;
		this.groupbyList = groupbyList;
		
		this.isTupleMapPresent = true;
		this.selectExpressionList = selectExpressionList;
		
	}

	public void resetStream() {
		// TODO Auto-generated method stub

	}

	/*
	 * read one tuple from the operator, iterate over the column
	 */
	public ArrayList<Datum[]> readOneTuple() {

		ArrayList<Datum[]> finalGroupByDatumArrayList = new ArrayList<Datum[]>();
		Datum[] readOneTupleFromOper = oper.readOneTuple();
//		printTuple(readOneTupleFromOper);
		Datum singleDatum;
		

		if (isTupleMapPresent) {
			TupleStruct.setTupleTableMap(readOneTupleFromOper);
			isTupleMapPresent = false;
		}

		int count = 0;
		ArrayList<String> datumColumnName = (ArrayList<String>) TupleStruct
				.getTupleTableMap();
		while (readOneTupleFromOper != null) {
			//System.out.println("NEW TUPLE READ");
			count++;
			StringBuilder mapKey = new StringBuilder();
			for (Column groupbyColumnName : groupbyList) {
				String grpColName = groupbyColumnName.getWholeColumnName()
						.toLowerCase();
				if (datumColumnName.contains(grpColName)) {
					// System.out.println("TESTTTTTT");
					int index = datumColumnName.indexOf(grpColName);
					// printTuple(readOneTupleFromOper);
					// System.out.println(index);
					singleDatum = readOneTupleFromOper[index];
					// System.out.println("GROUP BY COLUMN NAME/VALUE: "+singleDatum.getColumn().getColumnName().toLowerCase()+"/"+singleDatum.getStringValue());
					mapKey.append(singleDatum.getStringValue());
					// System.out.println(mapKey);
				} else {
					System.out.println("Not able to find a match"
							+ datumColumnName + " : " + grpColName);
					return null;
				}
			}

			// Building the datum[] from select item expressions
			Datum[] newSelectItemsArray = new Datum[selectExpressionList.size()];
			Map<Integer, String> fnMap = new HashMap<Integer, String>();
			for (int itr = 0; itr < selectExpressionList.size(); itr++) {
				// System.out.println("EXPRESSION"+countExpression);
				SelectExpressionItem newItem = selectExpressionList.get(itr);
				Expression e = newItem.getExpression();

				CalcTools calc = new CalcTools(readOneTupleFromOper);

				if (e instanceof Function) {
					// System.out.println("PRINT THERE IS A FUNCTION IN THE SELECT BODY");
					Function aggregateFunction = (Function) e;
					// aggregareFunctionList.add(aggregateFunction);
					String funcName = aggregateFunction.getName();
					fnMap.put(itr,funcName);
				} else {
					fnMap.put(itr, "col");
				}
				e.accept(calc);
				Datum tempDatum = getDatum(calc, newItem);
				newSelectItemsArray[itr] = tempDatum;
			}

			
			
			if (!groupByMap.containsKey(mapKey.toString())) {
				
				groupByMap.put(mapKey.toString(), newSelectItemsArray);
				
			} else {
				//System.out.println("KEY/VALUE PAIR ALREADY EXISTING IN MAP");
				Datum[] datumArray = groupByMap.get(mapKey.toString());
				Datum[] tempDatum = new Datum[datumArray.length];
				for (int i=0;i<datumArray.length;i++) {
					String funcName = fnMap.get(i);
					tempDatum[i] = getDatumFun(funcName, newSelectItemsArray[i], datumArray[i]);
					System.out.println(newSelectItemsArray[i] + " :: " + datumArray[i]+ " :: " + funcName + "::"+tempDatum[i]);
				}
				groupByMap.put(mapKey.toString(), tempDatum);

			}

			readOneTupleFromOper = this.oper.readOneTuple();
			
		}
		finalGroupByDatumArrayList.addAll(groupByMap.values());
		return finalGroupByDatumArrayList;
	}

	private Datum getDatumFun(String funcName, Datum t1,
			Datum t2) {
		switch(funcName.toLowerCase()){
		case "col":
			return t1;
		case "sum":
//			System.out.println("AGGREGATE FUNC - SUM");
			return sum(t1,t2);
		case "count":
//			System.out.println("AGGREGATE FUNC - COUNT method");
			return sum(t1,t2);
		case "min":
//			System.out.println("MIN method");
			return min(t1,t2);
		case "max":
//			System.out.println("MAX method");
			return max(t1,t2);
		case "avg":
//			System.out.println("AVG method");
			return avg(t1,t2);
		case "stdev":
			System.out.println("STDEV method... Not handled");
			return null;
		default:
			System.out.println("AGGREGATE FUNCTION NOT MATCHED" + funcName);
			return null;
		}
	}



	public void printTestMap(Map groupMap) {
		System.out.println("SIZE OF THE MAP" + groupByMap.size());
		for (Entry<String, Datum[]> entry : groupByMap.entrySet()) {
			System.out.println("Key = " + entry.getKey() + ", Value = "
					+ entry.getValue() + "value size: "
					+ entry.getValue().length);
		}
	}

	// public void printTestMap() {
	// // Iterate over the map to get the key; for each key, iterate over the
	// // value i.e. ArrayList<Datum[]>, get the datum[] and pass it to
	// // aggregate operator
	// // create Aggregate operator
	// for (Entry<String, ArrayList<Datum[]>> entry : groupByMap.entrySet()) {
	// ArrayList<Datum[]> MapValueList = entry.getValue();
	// for (Datum[] singleDatumArray : MapValueList) {
	// System.out
	// .println("PRINTING DATUM[] before passing to TEST FUCNTION");
	// printTuple(singleDatumArray);
	//
	// // Datum[] aggregateDatumArray =
	// // test.aggregateFunction(singleDatumArray);
	// System.out
	// .println("-------------------------------------------------");
	// // printTuple(aggregateDatumArray);
	// }
	// test.resetAggregateDatumBuffer();
	// // System.out.println("Key = " + entry.getKey() + ", Value = " +
	// // entry.getValue()+"value size: "+entry.getValue().size());
	// }
	// }

	private Datum getDatum(CalcTools calc, SelectExpressionItem newItem) {
		Column newCol = null;
		Object calcOut = calc.getResult();
		if (newItem.getAlias() != null) {
			// System.out.println("Alias");
			newCol = new Column(null, newItem.getAlias());
		} else {
			// System.out.println("Exp");
			newCol = calc.getColumn();
			// System.out.println(newCol.getColumnName());
		}
		Datum tempDatum = null;
		if (calcOut instanceof Long) {
			String value = calcOut.toString();
			// String valueWithFuncName = funcName.concat("("+value+")");
			tempDatum = new Datum.dLong(calcOut.toString(), newCol);
			// System.out.println("CALC OUTPUT FOR dLONG: "+calcOut.toString());
			// System.out.println("CALC OUTPUT FOR dLONG: "+newCol);
			// System.out.println("NEW FUNC VALUE: "+valueWithFuncName);
			// System.out.println("tempDatum: "+tempDatum.getStringValue());
		} else if (calcOut instanceof String) {
			tempDatum = new Datum.dString((String) calcOut, newCol);
			// System.out.println("CALC OUTPUT FOR DSTRING: "+(String) calcOut);
			// System.out.println("tempDatum"+tempDatum.getStringValue());
		} else if (calcOut instanceof Date) {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			tempDatum = new Datum.dDate(df.format(calcOut), newCol);
			// System.out.println("tempDatum"+tempDatum.getStringValue());

		} else if (calcOut instanceof Double) {
			Double value = (Double) calcOut;
		}
		return tempDatum;
	}

	private void printTuple(Datum[] row) {
		if (row != null && row.length != 0) {
			for (Datum col : row) {
				System.out.print(col + "|");
			}
			System.out.println("");
		}
		System.out.println("------------------------------------------------");
	}

	public Datum sum(Datum t1, Datum t2) {
		if (t1 instanceof dLong) {
			long value1 = ((dLong) t1).getValue();
			long value2 = ((dLong) t2).getValue();
			return new Datum.dLong(String.valueOf(value1 + value2),
					t1.getColumn());
		} else if (t1 instanceof dString) {
			String value1 = ((dString) t1).getValue();
			String value2 = ((dString) t2).getValue();
			return new Datum.dString(String.valueOf(value1 + value2),
					t1.getColumn());
		} else if (t1 instanceof dDate) {
			System.out.println("Date not handled !!! in sum");
			return null;
		} else if (t1 instanceof dDecimal) {
			Double value1 = ((dDecimal) t1).getValue();
			Double value2 = ((dDecimal) t2).getValue();
			return new Datum.dString(String.valueOf(value1 + value2),
					t1.getColumn());
		} else {
			System.out.println("Unknown datatype not handled !!! in sum");
			return null;
		}

	}
	
	private Datum avg(Datum t1, Datum t2) {
		if (t1 instanceof dLong) {
			long value1 = ((dLong) t1).getValue();
			long value2 = ((dLong) t2).getValue();
			return new Datum.dLong(String.valueOf(value1 + value2),
					t1.getColumn());
		} else if (t1 instanceof dString) {
			String value1 = ((dString) t1).getValue();
			String value2 = ((dString) t2).getValue();
			return new Datum.dString(String.valueOf(value1 + value2),
					t1.getColumn());
		} else if (t1 instanceof dDate) {
			System.out.println("Date not handled !!! in sum");
			return null;
		} else if (t1 instanceof dDecimal) {
			Double value1 = ((dDecimal) t1).getValue();
			Double value2 = ((dDecimal) t2).getValue();
			return new Datum.dString(String.valueOf(value1 + value2),
					t1.getColumn());
		} else {
			System.out.println("Unknown datatype not handled !!! in sum");
			return null;
		}
	}

	public Datum min(Datum t1, Datum t2) {
		if (t1 instanceof dLong) {
			Long value1 = ((dLong) t1).getValue();
			Long value2 = ((dLong) t2).getValue();
			int compare = value1.compareTo(value2);
			if (compare <= 0)
				return t1;
			else
				return t2;

		} else if (t1 instanceof dString) {
			String value1 = ((dString) t1).getValue();
			String value2 = ((dString) t2).getValue();
			int compare = value1.compareTo(value2);
			if (compare <= 0)
				return t1;
			else
				return t2;
		} else if (t1 instanceof dDate) {
			Date value1 = ((dDate) t1).getValue();
			Date value2 = ((dDate) t2).getValue();
			int compare = value1.compareTo(value2);
			if (compare <= 0)
				return t1;
			else
				return t2;
		} else if (t1 instanceof dDecimal) {
			Double value1 = ((dDecimal) t1).getValue();
			Double value2 = ((dDecimal) t2).getValue();
			int compare = value1.compareTo(value2);
			if (compare <= 0)
				return t1;
			else
				return t2;
		} else {
			System.out.println("Unknown datatype not handled !!! in min");
			return null;
		}
	}

	public Datum max(Datum t1, Datum t2) {
		if (t1 instanceof dLong) {
			Long value1 = ((dLong) t1).getValue();
			Long value2 = ((dLong) t2).getValue();
			int compare = value1.compareTo(value2);
			if (compare >= 0)
				return t1;
			else
				return t2;

		} else if (t1 instanceof dString) {
			String value1 = ((dString) t1).getValue();
			String value2 = ((dString) t2).getValue();
			int compare = value1.compareTo(value2);
			if (compare >= 0)
				return t1;
			else
				return t2;
		} else if (t1 instanceof dDate) {
			Date value1 = ((dDate) t1).getValue();
			Date value2 = ((dDate) t2).getValue();
			int compare = value1.compareTo(value2);
			if (compare >= 0)
				return t1;
			else
				return t2;
		} else if (t1 instanceof dDecimal) {
			Double value1 = ((dDecimal) t1).getValue();
			Double value2 = ((dDecimal) t2).getValue();
			int compare = value1.compareTo(value2);
			if (compare >= 0)
				return t1;
			else
				return t2;
		} else {
			System.out.println("Unknown datatype not handled !!! in min");
			return null;
		}
	}

}
