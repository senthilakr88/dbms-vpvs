package edu.buffalo.cse562.sql.expression.evaluator;

import java.util.List;

import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.structure.Datum;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

public class EqualityCheck extends AbstractExpressionVisitor {

	boolean check = false;
	
	public Boolean getEquality() {
		return check;
	}
	
	public void visit(EqualsTo equalsTo) {
		check=true;
	}
	
	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}
}
