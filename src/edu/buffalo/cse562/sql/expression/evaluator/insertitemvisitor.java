package edu.buffalo.cse562.sql.expression.evaluator;

import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.structure.Datum.dLong;
import edu.buffalo.cse562.structure.Datum.dDate;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.Datum.dDecimal;
import edu.buffalo.cse562.structure.Datum.dString;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class insertitemvisitor extends AbstractExpressionVisitor implements
		ItemsListVisitor {

	Datum temp;
	Datum[] datum;
	List<Column> insertcols;
	List<Column> existingcols;
	List<String> existingType;
	Column col;
	String type;

	public insertitemvisitor(List<Column> column1, List<Column> column2, List<String> type) {
		this.insertcols = column1;
		this.existingcols = column2;
		this.existingType = type;
	}

	public insertitemvisitor(Column column, String type) {
		this.col = column;
		this.type = type;
	}

	public Datum[] getDatum() {
		return datum;
	}

	public Datum getTempDatum() {
		return temp;
	}

	@Override
	public void visit(SubSelect ss) {
		System.out
				.println("subselect not implemented via itemlistvisitor for inserts ");

	}

	@Override
	public void visit(ExpressionList el) {
		// System.out.println("el :: " + el);
		// System.out.println("then :: "+el.getExpressions());
		// System.out.println("then :: "+el.getExpressions().get(0).getClass());
		// insertitemvisitor a = new insertitemvisitor();
		// ((Expression)el.getExpressions().get(5)).accept(a);
		// // System.out.println("then :: "+;
		// //System.out.println("then :: "+((StringValue)el.getExpressions().get(8)).getValue());
		// System.out.println("then :: "+el.getExpressions().get(10).getClass());
		List<Expression> exprs = el.getExpressions();
		datum = new Datum[existingcols.size()];
		for (int i = 0; i < existingcols.size(); i++) {
			Column useCol = existingcols.get(i);
			String useType = existingType.get(i).toLowerCase().replaceAll("[^a-z]","").trim();
			int index = TupleStruct.getColIndex(insertcols, useCol);
//			System.out.println(index);
			insertitemvisitor iiv = new insertitemvisitor(useCol,useType);
			exprs.get(index).accept(iiv);
			datum[i] = iiv.getTempDatum();
//			printTuple(datum[i]);
		}
//		System.out.println("printing tuple @ iiv :: ");
//		printTupleCol(datum);
	}

	

	public void visit(StringValue stringValue) {
		String str = stringValue.getNotExcapedValue();
		if(type.equals("date"))
            temp = new dDate(str, col);
		else
        	temp = new dString(str, col);
	}

	public void visit(LongValue longValue) {
		if(type.equals("int"))
			temp = new dLong(longValue.getValue(), col);
		else
			temp = new dDecimal(Double.parseDouble(longValue.getStringValue()), col, 4);
	}

	public void visit(DoubleValue doubleValue) {
		if(type.equals("decimal"))
			temp = new dDecimal(doubleValue.getValue(), col, 4);
		else
			temp = new dLong(Long.parseLong(doubleValue.toString()), col);
	}

	public void visit(Function fnctn) {
		String functionName = fnctn.getName().toLowerCase();
		ExpressionList parameters = fnctn.getParameters();
		List expressionList = null;
		if (parameters != null) {
			expressionList = parameters.getExpressions();
		}
		java.lang.reflect.Method method = null;
		try {
			method = this.getClass().getMethod(functionName, List.class);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		}
		try {
			method.invoke(this, expressionList);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
	
	public void date(List eList) {
		insertitemvisitor iiv = new insertitemvisitor(this.col,this.type);
		Expression e = (Expression) eList.get(0);
        e.accept(iiv);
        this.temp = iiv.getTempDatum();
	}
	
	private void printTuple(Datum[] row) {
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
			System.out.println();
		}
	}
	
	private void printTupleCol(Datum[] row) {
		Boolean first = true;
		if (row != null && row.length != 0) {
			for (Datum col : row) {
				if (!first)
					System.out.print("|" + col.getColumn());
				else {
					System.out.print(col.getColumn());
					first = false;
				}
			}
			System.out.println();
		}
	}
	
	private void printTuple(Datum row) {
		if (row != null) {
			System.out.println(row);
		}
		
	}

}
