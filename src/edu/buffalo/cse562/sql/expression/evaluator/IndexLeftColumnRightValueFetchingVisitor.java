package edu.buffalo.cse562.sql.expression.evaluator;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import edu.buffalo.cse562.structure.Datum;

public class IndexLeftColumnRightValueFetchingVisitor extends AbstractExpressionVisitor {
	Column leftColumn;
	Column rightColumn;
	Table leftTable;
	Table rightTable;
	Column column;
	Table table;
	Datum[] tempTuple;
	String rightTableS;
	String functionName;
	List<Expression> functionExpressionList;
	Datum singleDatumValue;
	String expressionOperator;
	String rightVisitorType;
	Boolean isVisitorFunction = false;
	Boolean isVisitorLong = false;
	Boolean isVisitorColumn = false;
	Boolean isVisitorDate = false;
	Boolean isVisitorString = false;
	Boolean isVisitorEqualsTo = false;
	
	public Column getLeftCol() {
		return leftColumn;
	}
	
	public Column getRightCol() {
		return rightColumn;
	}
	
	public Datum getValue() {
//		System.out.println("DATUM VALUE FROM VISITOR: "+singleDatumValue);
		return singleDatumValue;
	}
	
	public Column getCol() {
		return column;
	}
	
	public Table getTable() {
		return table;
	}
	
	public Table getLeftTab() {
		return leftTable;
	}
	
	public Table getRightTab() {
		return rightTable;
	}
	
	public String getExressionOperator(){
		return expressionOperator;
	}
	
	public void visit(Column column) {
		isVisitorColumn = true;
		this.column = column;
	}
	
	public void visit(Function function) {
//		System.out.println("Function");
		isVisitorFunction = true;
		functionName = function.getName().toLowerCase();
		ExpressionList functionExpression = function.getParameters();
		functionExpressionList = functionExpression.getExpressions();
	}
	
	public void visit(DateValue date) {
//		System.out.println("Date fun");
		isVisitorDate=true;
		Date dateValue = date.getValue();
		Column datumColumn = new Column();
		singleDatumValue = new Datum.dDate(dateValue, datumColumn);
//		System.out.println("Date value :: "+singleDatumValue.toString());
	}
	
	public void visit(StringValue string) {
//		System.out.println("Stringvalue");
		isVisitorString = true;
		String stringValue = string.getValue();
		Column datumColumn = new Column();
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
            format.parse(stringValue);
            singleDatumValue = new Datum.dDate(stringValue, datumColumn);
        } catch (ParseException e) {
        	singleDatumValue = new Datum.dString(stringValue, datumColumn);
        }
		
//		System.out.println("String value :: "+singleDatumValue.toString());
	}
	
	public void visit(LongValue value) {
//		System.out.println("Longvalue");
		isVisitorLong = true;
		Long longValue = value.getValue();
		Column datumColumn = new Column();
		singleDatumValue = new Datum.dLong(longValue, datumColumn);
//		System.out.println(singleDatumValue.toString());
	}
	
	
	public void visit(EqualsTo equalsTo) {
//		System.out.println("EqualsTo is handled");
		expressionOperator = "=";
		equalsTo.getLeftExpression().accept(this);
		
		if(isVisitorColumn){
//			System.out.println("LEFT EXPRESSION IS COLUMN");
			leftColumn = column;
			leftTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorFunction){
//			System.out.println("LEFT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
				if(functionExpressionList.size()<1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			isVisitorFunction = false;
		}
		else if(isVisitorLong){
//			System.out.println("LEFT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		else{
			System.out.println("LEFT EXPRESSION IS NOT COLUMN, FUNCTION OR LONG VALUE");
		}
		equalsTo.getRightExpression().accept(this);
		if(isVisitorFunction){
//			System.out.println("RIGHT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
//				System.out.println("FUNCTION EXPRESSION SIZE" + functionExpressionList.size());
				if(functionExpressionList.size() <= 1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			else{
				System.out.println("Function name other than date is NOT handled");
			}
			isVisitorFunction = false;
		}
		else if(isVisitorColumn){
//			System.out.println("RIGHT EXPRESSION IS COLUMN");
			rightColumn = column;
			rightTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorLong){
//			System.out.println("RIGHT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		else if(isVisitorString){
//			System.out.println("RIGHT EXPRESSION IS STRING VALUE");	
			isVisitorString = false;
		}
//		System.out.println(singleDatumValue);
		functionExpressionList = null;
	}
	
	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}
	
	public void visit(MinorThan minorThanExpression) {
		expressionOperator = "<";
		minorThanExpression.getLeftExpression().accept(this);
		if(isVisitorColumn){
//			System.out.println("LEFT EXPRESSION IS COLUMN");
			leftColumn = column;
			leftTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorFunction){
//			System.out.println("LEFT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
				if(functionExpressionList.size()<1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			isVisitorFunction = false;
		}
		else if(isVisitorLong){
//			System.out.println("LEFT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		minorThanExpression.getRightExpression().accept(this);
		if(isVisitorFunction){
//			System.out.println("RIGHT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
//				System.out.println("FUNCTION EXPRESSION SIZE" + functionExpressionList.size());
				if(functionExpressionList.size() <= 1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			else{
				System.out.println("Function name other than date is NOT handled");
			}
			isVisitorFunction = false;
		}
		else if(isVisitorColumn){
//			System.out.println("RIGHT EXPRESSION IS COLUMN");
			rightColumn = column;
			rightTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorLong){
//			System.out.println("RIGHT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		else if(isVisitorString){
//			System.out.println("RIGHT EXPRESSION IS STRING VALUE");	
			isVisitorString = false;
		}
//		System.out.println(singleDatumValue);
		functionExpressionList = null;
	}
	
	public void visit(MinorThanEquals minorThanEqualsExpression) {
		expressionOperator = "<=";
		minorThanEqualsExpression.getLeftExpression().accept(this);
		if(isVisitorColumn){
//			System.out.println("LEFT EXPRESSION IS COLUMN");
			leftColumn = column;
			leftTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorFunction){
//			System.out.println("LEFT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
				if(functionExpressionList.size()<1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			isVisitorFunction = false;
		}
		else if(isVisitorLong){
//			System.out.println("LEFT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		minorThanEqualsExpression.getRightExpression().accept(this);
		if(isVisitorFunction){
//			System.out.println("RIGHT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
//				System.out.println("FUNCTION EXPRESSION SIZE" + functionExpressionList.size());
				if(functionExpressionList.size() <= 1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			else{
				System.out.println("Function name other than date is NOT handled");
			}
			isVisitorFunction = false;
		}
		else if(isVisitorColumn){
//			System.out.println("RIGHT EXPRESSION IS COLUMN");
			rightColumn = column;
			rightTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorLong){
//			System.out.println("RIGHT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		else if(isVisitorString){
//			System.out.println("RIGHT EXPRESSION IS STRING VALUE");	
			isVisitorString = false;
		}
//		System.out.println(singleDatumValue);
		functionExpressionList = null;
	}
	
	public void visit(GreaterThan greaterThanExpression) {
		expressionOperator = ">";
		greaterThanExpression.getLeftExpression().accept(this);
		if(isVisitorColumn){
//			System.out.println("LEFT EXPRESSION IS COLUMN");
			leftColumn = column;
			leftTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorFunction){
//			System.out.println("LEFT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
				if(functionExpressionList.size()<1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			isVisitorFunction = false;
		}
		else if(isVisitorLong){
//			System.out.println("LEFT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		greaterThanExpression.getRightExpression().accept(this);
		if(isVisitorFunction){
//			System.out.println("RIGHT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
//				System.out.println("FUNCTION EXPRESSION SIZE" + functionExpressionList.size());
				if(functionExpressionList.size() <= 1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			else{
				System.out.println("Function name other than date is NOT handled");
			}
			isVisitorFunction = false;
		}
		else if(isVisitorColumn){
//			System.out.println("RIGHT EXPRESSION IS COLUMN");
			rightColumn = column;
			rightTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorLong){
//			System.out.println("RIGHT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		else if(isVisitorString){
//			System.out.println("RIGHT EXPRESSION IS STRING VALUE");	
			isVisitorString = false;
		}
//		System.out.println(singleDatumValue);
		functionExpressionList = null;		
	}
	
	public void visit(GreaterThanEquals greaterThanEqualsExpression) {
		expressionOperator = ">=";
		greaterThanEqualsExpression.getLeftExpression().accept(this);
		if(isVisitorColumn){
//			System.out.println("LEFT EXPRESSION IS COLUMN");
			leftColumn = column;
			leftTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorFunction){
//			System.out.println("LEFT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
				if(functionExpressionList.size()<1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			isVisitorFunction = false;
		}
		else if(isVisitorLong){
//			System.out.println("LEFT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		greaterThanEqualsExpression.getRightExpression().accept(this);
		if(isVisitorFunction){
//			System.out.println("RIGHT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
//				System.out.println("FUNCTION EXPRESSION SIZE" + functionExpressionList.size());
				if(functionExpressionList.size() <= 1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			else{
				System.out.println("Function name other than date is NOT handled");
			}
			isVisitorFunction = false;
		}
		else if(isVisitorColumn){
//			System.out.println("RIGHT EXPRESSION IS COLUMN");
			rightColumn = column;
			rightTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorLong){
//			System.out.println("RIGHT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		else if(isVisitorString){
//			System.out.println("RIGHT EXPRESSION IS STRING VALUE");	
			isVisitorString = false;
		}
//		System.out.println(singleDatumValue);
		functionExpressionList = null;
	}
	
	public void visit(NotEqualsTo notEqualsTo) {
		expressionOperator = "<>";
		notEqualsTo.getLeftExpression().accept(this);
		if(isVisitorColumn){
//			System.out.println("LEFT EXPRESSION IS COLUMN");
			leftColumn = column;
			leftTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorFunction){
//			System.out.println("LEFT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
				if(functionExpressionList.size()<1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			isVisitorFunction = false;
		}
		else if(isVisitorLong){
//			System.out.println("LEFT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		notEqualsTo.getRightExpression().accept(this);
		if(isVisitorFunction){
//			System.out.println("RIGHT EXPRESSION IS FUNCTION");
			if(functionName.equalsIgnoreCase("date")){
//				System.out.println("FUNCTION EXPRESSION SIZE" + functionExpressionList.size());
				if(functionExpressionList.size() <= 1){
					Expression e = functionExpressionList.get(0);
					e.accept(this);
				}
				else{
					System.out.println("Expression list more than 1 is NOT handled at the moment");
				}
			}
			else{
				System.out.println("Function name other than date is NOT handled");
			}
			isVisitorFunction = false;
		}
		else if(isVisitorColumn){
//			System.out.println("RIGHT EXPRESSION IS COLUMN");
			rightColumn = column;
			rightTable = column.getTable();
			isVisitorColumn = false;
		}
		else if(isVisitorLong){
//			System.out.println("RIGHT EXPRESSION IS LONG VALUE");	
			isVisitorLong = false;
		}
		else if(isVisitorString){
//			System.out.println("RIGHT EXPRESSION IS STRING VALUE");	
			isVisitorString = false;
		}
//		System.out.println(singleDatumValue);
		functionExpressionList = null;
	}	
}