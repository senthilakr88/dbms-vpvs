package edu.buffalo.cse562.physicalPlan;

/*
 * to do
 * 1. pass the column name to the sum method
 * 2. loop readOneTuple method read all tuple as a datum[] from where (call readOneTuple method on select operator object)
 * 3. find the duplicate in the column (group by - column name) passed and compute the expression on the 
 * 4. Keep a map: key as the column name and value as the computed value (This has to be done as a buffer while reading the tuples. Ex: sum can be adding the values)
 * 
 */


import java.util.List;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

public class AggregateOperator implements Operator{
	// Select body and the groupby column name as the fields in the object that is passed to the visitor object
	Operator oper;
	SelectBody selectBody;
	List<Column> groupByColumnReferences;
	List<Column> tableColumnList;
	Test t;
	
	public AggregateOperator(Operator oper,SelectBody selectBody, List<Column> tableColumnList){
		this.selectBody = selectBody;
		groupByColumnReferences = ((PlainSelect) selectBody).getGroupByColumnReferences();
		this.tableColumnList = tableColumnList;
		this.oper = oper;
		t = new Test(oper, groupByColumnReferences, tableColumnList);
	}
	
	
	@Override
	public void resetStream() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Datum[] readOneTuple() {
		List<SelectItem> listOfSelectItems = ((PlainSelect) selectBody).getSelectItems();
		Datum[] datumToBeReturned;
		//iterate over the list of select items such as A, B, SUM(C), MAX(D)
		for(SelectItem itr:listOfSelectItems){
			SelectExpressionItem singleElement = (SelectExpressionItem) itr;	
			Expression aggregateExpression = singleElement.getExpression();
			Function aggregateFunction = (Function) aggregateExpression;
			aggregateFunction.accept(t);
			t.visit(aggregateFunction);
		}
		return readOneTuple();
	}

}

	