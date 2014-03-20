package edu.buffalo.cse562.sql.expression.evaluator;

import java.util.List;

import edu.buffalo.cse562.physicalPlan.Datum;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

public class ColumnFetcher extends AbstractExpressionVisitor {

	Column leftColumn;
	Column rightColumn;
	Column column;
	
	public Column getLeftCol() {
		return leftColumn;
	}
	
	public Column getRightCol() {
		return rightColumn;
	}
	
	public void visit(Column column) {
		this.column = column;
	}
	
	public void visit(EqualsTo equalsTo) {
		equalsTo.getLeftExpression().accept(this);
		leftColumn = column;
		equalsTo.getRightExpression().accept(this);
		rightColumn = column;
	}
	
	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}
}
