package edu.buffalo.cse562.sql.expression.evaluator;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.Datum.dDate;
import edu.buffalo.cse562.structure.Datum.dDecimal;
import edu.buffalo.cse562.structure.Datum.dLong;
import edu.buffalo.cse562.structure.Datum.dString;

public class columnfinder extends AbstractExpressionVisitor {

	private String column;


	@Override
	public void visit(Addition addition) {
		addition.getLeftExpression().accept(this);
		addition.getRightExpression().accept(this);
	}
	
	public String getColumn(){
			return column;
	}
	
	public void visit(Column column) {
		this.column = column.getWholeColumnName();
	}

	@Override
	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	@Override
	public void visit(Division division) {
		division.getLeftExpression().accept(this);
		division.getRightExpression().accept(this);
	}

	@Override
	public void visit(DoubleValue doubleValue) {
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		equalsTo.getLeftExpression().accept(this);
		equalsTo.getRightExpression().accept(this);
	}
	
	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		notEqualsTo.getLeftExpression().accept(this);
		notEqualsTo.getRightExpression().accept(this);
	}

    @Override                                                                                                                                                             
    public void visit(Function function) {
        String functionName = function.getName().toLowerCase();
        ExpressionList parameters = function.getParameters();
    	List expressionList = null;
        if(parameters!=null){
        	expressionList = parameters.getExpressions();
        }
        java.lang.reflect.Method method=null;
        try {
          method = this.getClass().getMethod(functionName, List.class);
        } catch (SecurityException e) {
          // ...
        } catch (NoSuchMethodException e) {
          // ...
        }
        try {
              method.invoke(this, expressionList);
            } catch (IllegalArgumentException e) {
            } catch (IllegalAccessException e) {
            } catch (InvocationTargetException e) {
            }
    }
    
    public void sum(List eList) {
    }
    
    public void avg(List eList) {
    }
    
    public void count(List eList) {
    }
    
    public void min(List eList) {
    }
    
    public void max(List eList) {
    }
    
    public void date(List eList) {
    }

	@Override
	public void visit(GreaterThan greaterThan) {
		greaterThan.getLeftExpression().accept(this);
		greaterThan.getRightExpression().accept(this);
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
			greaterThanEquals.getLeftExpression().accept(this);
			greaterThanEquals.getRightExpression().accept(this);
		}

	@Override
	public void visit(InverseExpression inverseExpression) {
		inverseExpression.getExpression().accept(this);
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		likeExpression.getLeftExpression().accept(this);
		likeExpression.getRightExpression().accept(this);
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		existsExpression.getRightExpression().accept(this);
	}

	@Override
	public void visit(LongValue longValue) {
	}

	@Override
	public void visit(Object longValue) {
	}

	@Override
	public void visit(MinorThan minorThan) {
		minorThan.getLeftExpression().accept(this);
		minorThan.getRightExpression().accept(this);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		minorThanEquals.getLeftExpression().accept(this);
		minorThanEquals.getRightExpression().accept(this);
	}

	@Override
	public void visit(Multiplication multiplication) {
		multiplication.getLeftExpression().accept(this);
		multiplication.getRightExpression().accept(this);
	}


	@Override
	public void visit(NullValue nullValue) {

	}

	@Override
	public void visit(OrExpression orExpression) {
		orExpression.getLeftExpression().accept(this);
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue stringValue) {
	}

	@Override
	public void visit(Subtraction subtraction) {
		subtraction.getLeftExpression().accept(this);
		subtraction.getRightExpression().accept(this);
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		binaryExpression.getLeftExpression().accept(this);
		binaryExpression.getRightExpression().accept(this);
	}

	/*
	 * @Override public void visit(ExpressionList expressionList) { for
	 * (Iterator iter = expressionList.getExpressions().iterator();
	 * iter.hasNext();) { Expression expression = (Expression) iter.next();
	 * expression.accept(this); }
	 * 
	 * }
	 */

	@Override
	public void visit(DateValue dateValue) {
	}

	@Override
	public void visit(TimestampValue timestampValue) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(TimeValue timeValue) {
		throw new UnsupportedOperationException("Not supported yet.");
	}
	

	@Override
	public void visit(CaseExpression caseExpression) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(WhenClause whenClause) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/*
	 * @Override public void visit(AllComparisonExpression
	 * allComparisonExpression) {
	 * allComparisonExpression.getSubSelect().getSelectBody().accept(this); }
	 * 
	 * @Override public void visit(AnyComparisonExpression
	 * anyComparisonExpression) {
	 * anyComparisonExpression.getSubSelect().getSelectBody().accept(this); }
	 * 
	 * @Override public void visit(SubJoin subjoin) {
	 * subjoin.getLeft().accept(this);
	 * subjoin.getJoin().getRightItem().accept(this); }
	 */
	@Override
	public void visit(Concat concat) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Matches mtchs) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(BitwiseAnd ba) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(BitwiseOr bo) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(BitwiseXor bx) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}