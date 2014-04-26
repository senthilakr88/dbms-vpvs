package edu.buffalo.cse562.sql.expression.evaluator;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.structure.Datum;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

public class AndVisitor extends AbstractExpressionVisitor {

	List<Expression> expList = new ArrayList<Expression>();
	
	public void visit(AndExpression and) {
//		System.out.println("Right is " + and.getRightExpression());
		while(and.getRightExpression()!=null){
			expList.add(and.getRightExpression());
//			System.out.println("LEFT is " + and.getLeftExpression());
			if (and.getLeftExpression() instanceof AndExpression){
				and = (AndExpression) and.getLeftExpression();
			} else {
				expList.add(and.getLeftExpression());
				break;
			}
		}
	}
	
	public List<Expression> getList() {
		return expList;
	}
	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}
}
