package edu.buffalo.cse562.sql.expression.evaluator;

//import java.util.List;
import java.util.logging.Level;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
//import net.sf.jsqlparser.schema.*;
//import net.sf.jsqlparser.statement.select.*;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.Tuple;

/**
 *
 * @author Niccolo' Meneghetti
 */
public class CalcTools extends AbstractExpressionVisitor
{
    private Object accumulator;
    private boolean accumulatorBoolean;
//    private List columns;
    logManager lg = new logManager();
    Tuple t;
    public Object getResult()
    
    { 	
        //lg.logger.log(Level.INFO, String.valueOf(accumulator));
    	return accumulator; }

    public CalcTools(Tuple t2) {
		// TODO Auto-generated constructor stub
    	t = t2;
    	
	}

	@Override
    public void visit(Addition addition) {
		//lg.logger.log(Level.INFO, "Came to addition");
        addition.getLeftExpression().accept(this);
        Object leftValue = accumulator;
        addition.getRightExpression().accept(this);
        Object rightValue = accumulator;
        accumulator = Double.parseDouble(leftValue.toString()) + Double.parseDouble(rightValue.toString());
        //lg.logger.log(Level.INFO, "Addition result is"+accumulator.toString());
    }

    public void visit(Column column) {
        //lg.logger.log(Level.INFO, "Came to get column name"+column.getWholeColumnName());
        accumulator = String.valueOf(t.valueOf((column.getColumnName())));
        //lg.logger.log(Level.INFO, String.valueOf(accumulator));
    }
    
    @Override
    public void visit(AndExpression andExpression) {
        //lg.logger.log(Level.INFO, "Came to and expression");
        andExpression.getLeftExpression().accept(this);
        boolean leftValue = accumulatorBoolean;
        andExpression.getRightExpression().accept(this);
        boolean rightValue = accumulatorBoolean;
        if (leftValue && rightValue){
            //lg.logger.log(Level.INFO, "TRUE");
        	accumulatorBoolean=true;
        } else {
        	//lg.logger.log(Level.INFO, "FALSE");
        	accumulatorBoolean=false;
        }
    }

    @Override
    public void visit(Between between) {
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

  /*  @Override
    public void visit(Column tableColumn) {
        if (!(columns == null)) {
            columns.add(tableColumn.getWholeColumnName());
        }
        if (!(metaDataColumns == null)) {
            meta = new NaviSoftRealMeta();
            nameColumn = tableColumn.getColumnName();
            meta.setTable(tableColumn.getTable().getName());
            meta.setFieldName(nameColumn);
        }
    }
*/
    @Override
    public void visit(Division division) {
		//lg.logger.log(Level.INFO, "Came to addition");
        division.getLeftExpression().accept(this);
        Object leftValue = accumulator;
        division.getRightExpression().accept(this);
        Object rightValue = accumulator;
        accumulator = Double.parseDouble(leftValue.toString())/Double.parseDouble(rightValue.toString());
        //lg.logger.log(Level.INFO, "Division result is"+accumulator.toString());
    }

    @Override
    public void visit(DoubleValue doubleValue) {
    	
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        //lg.logger.log(Level.INFO, "Came to greater than");
        equalsTo.getLeftExpression().accept(this);
        Object leftValue = accumulator;
        equalsTo.getRightExpression().accept(this);
        Object rightValue = accumulator;
        if (Double.parseDouble(leftValue.toString())==Double.parseDouble(rightValue.toString())){
            //lg.logger.log(Level.INFO, "GREATER GREATER");

        	accumulatorBoolean=true;
        } else {
        	//lg.logger.log(Level.INFO, "NOT NOT NOT GREATER");
        	accumulatorBoolean=false;
        }
    }

    @Override
    public void visit(Function function) {

    }

    @Override
    public void visit(GreaterThan greaterThan) {
        //lg.logger.log(Level.INFO, "Came to greater than");
        greaterThan.getLeftExpression().accept(this);
        Object leftValue = accumulator;
        greaterThan.getRightExpression().accept(this);
        Object rightValue = accumulator;
        if (Double.parseDouble(leftValue.toString())>Double.parseDouble(rightValue.toString())){
            //lg.logger.log(Level.INFO, "GREATER GREATER");

        	accumulatorBoolean=true;
        } else {
        	//lg.logger.log(Level.INFO, "NOT NOT NOT GREATER");
        	accumulatorBoolean=false;
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        //lg.logger.log(Level.INFO, "Came to greater than equals");
        greaterThanEquals.getLeftExpression().accept(this);
        Object leftValue = accumulator;
        greaterThanEquals.getRightExpression().accept(this);
        Object rightValue = accumulator;
        if (Double.parseDouble(leftValue.toString())>=Double.parseDouble(rightValue.toString())){
            //lg.logger.log(Level.INFO, "GREATER GREATER");

        	accumulatorBoolean=true;
        } else {
        	//lg.logger.log(Level.INFO, "NOT NOT NOT GREATER");
        	accumulatorBoolean=false;
        }
    }

    /*@Override
    public void visit(InExpression inExpression) {
        inExpression.getLeftExpression().accept(this);
        inExpression.getItemsList().accept(this);
    }*/

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
        visitBinaryExpression(likeExpression);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        existsExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(LongValue longValue) {
    	accumulator=longValue.getValue(); 
    	//lg.logger.log(Level.INFO, "Returning long value"+accumulator.toString());
    }
    
    @Override
    public void visit(Object longValue) {
    	accumulator=longValue; 
    	//lg.logger.log(Level.INFO, "Returning object value"+accumulator.toString());
    }

    @Override
    public void visit(MinorThan minorThan) {
        //lg.logger.log(Level.INFO, "Came to greater than equals");
        minorThan.getLeftExpression().accept(this);
        Object leftValue = accumulator;
        minorThan.getRightExpression().accept(this);
        Object rightValue = accumulator;
        if (Double.parseDouble(leftValue.toString())<Double.parseDouble(rightValue.toString())){
            //lg.logger.log(Level.INFO, "MINOR");

        	accumulatorBoolean=true;
        } else {
        	//lg.logger.log(Level.INFO, "NOT NOT NOT MINOR");
        	accumulatorBoolean=false;
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        //lg.logger.log(Level.INFO, "Came to minor than equals");
        minorThanEquals.getLeftExpression().accept(this);
        Object leftValue = accumulator;
        minorThanEquals.getRightExpression().accept(this);
        Object rightValue = accumulator;
        if (Double.parseDouble(leftValue.toString())<=Double.parseDouble(rightValue.toString())){
            //lg.logger.log(Level.INFO, "minor than equals");

        	accumulatorBoolean=true;
        } else {
        	//lg.logger.log(Level.INFO, "NOT minor than equals");
        	accumulatorBoolean=false;
        }
    }

    @Override
    public void visit(Multiplication multiplication) {
		//lg.logger.log(Level.INFO, "Came to multiplication");
        multiplication.getLeftExpression().accept(this);
        Object leftValue = accumulator;
        multiplication.getRightExpression().accept(this);
        Object rightValue = accumulator;
        accumulator = Double.parseDouble(leftValue.toString()) + Double.parseDouble(rightValue.toString());
        //lg.logger.log(Level.INFO, "Multiplication result is"+accumulator.toString());
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(NullValue nullValue) {
    	
    }

    @Override
    public void visit(OrExpression orExpression) {
    	//lg.logger.log(Level.INFO, "Came to OR expression");
        orExpression.getLeftExpression().accept(this);
        boolean leftValue = accumulatorBoolean;
        orExpression.getRightExpression().accept(this);
        boolean rightValue = accumulatorBoolean;
        if (leftValue || rightValue){
            //lg.logger.log(Level.INFO, "TRUE");
        	accumulatorBoolean=true;
        } else {
        	//lg.logger.log(Level.INFO, "FALSE");
        	accumulatorBoolean=false;
        }
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
    	accumulator=stringValue.getValue(); 
    }

    @Override
    public void visit(Subtraction subtraction) {
    		//lg.logger.log(Level.INFO, "Came to subtraction");
            subtraction.getLeftExpression().accept(this);
            Object leftValue = accumulator;
            subtraction.getRightExpression().accept(this);
            Object rightValue = accumulator;
            accumulator = Double.parseDouble(leftValue.toString()) - Double.parseDouble(rightValue.toString());
            //lg.logger.log(Level.INFO, "Subtraction result is"+accumulator.toString());
      
    }

    public void visitBinaryExpression(BinaryExpression binaryExpression) {
    	//lg.logger.log(Level.INFO, "binary msg");
    	//binaryExpression.getRightExpression().accept(this);
    	binaryExpression.getLeftExpression().accept(this);
        binaryExpression.getRightExpression().accept(this);
    }

   /* @Override
    public void visit(ExpressionList expressionList) {
        for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
        }

    }*/

    @Override
    public void visit(DateValue dateValue) {
    	accumulator=dateValue.getValue(); 
    }

    @Override
    public void visit(TimestampValue timestampValue) {
    }

    @Override
    public void visit(TimeValue timeValue) {
    }

    @Override
    public void visit(CaseExpression caseExpression) {
    }

    @Override
    public void visit(WhenClause whenClause) {
    }
/*
    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        allComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        anyComparisonExpression.getSubSelect().getSelectBody().accept(this);
    }

    @Override
    public void visit(SubJoin subjoin) {
        subjoin.getLeft().accept(this);
        subjoin.getJoin().getRightItem().accept(this);
    }
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

	public boolean getAccumulatorBoolean() {
		return accumulatorBoolean;
	}

	public void setAccumulatorBoolean(boolean accumulatorBoolean) {
		this.accumulatorBoolean = accumulatorBoolean;
	}


   /* @Override
    public void visit(Parenthesis prnths) 
    { prnths.getExpression().accept(this); }
    */
    
}