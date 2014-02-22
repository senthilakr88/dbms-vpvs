package edu.buffalo.cse562.sql.expression.evaluator;

import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.Datum;
import edu.buffalo.cse562.physicalPlan.TupleStruct;

public class CalcTools extends AbstractExpressionVisitor {
	private Object accumulator;
	private boolean accumulatorBoolean;
	logManager lg = new logManager();
	Datum[] t;
	List<String> tupleTableMap;
	

	public Object getResult() {
		// lg.logger.log(Level.INFO, String.valueOf(accumulator));
		return accumulator;
	}

	public CalcTools(Datum[] t2) {
		t = t2;
		
	}

	@Override
	public void visit(Addition addition) {
		// lg.logger.log(Level.INFO, "Came to addition");
		addition.getLeftExpression().accept(this);
		Object leftValue = accumulator;
		addition.getRightExpression().accept(this);
		Object rightValue = accumulator;
		if (leftValue instanceof Double && rightValue instanceof Double) {
			accumulator = Double.parseDouble(leftValue.toString())
					+ Double.parseDouble(rightValue.toString());
		} else if (leftValue instanceof Long && rightValue instanceof Long) {
			accumulator = Long.parseLong(leftValue.toString())
					+ Long.parseLong(rightValue.toString());
		}
	}
	
	public void visit(Column column) {

		int index=0;
		
		tupleTableMap = TupleStruct.getTupleTableMap();
		if(tupleTableMap.contains(column.getWholeColumnName())) {
			index = tupleTableMap.indexOf(column.getWholeColumnName());
		}

		Datum row = t[index];
//		lg.logger.log(Level.INFO, index + ":" + row.toComString() + " : "
//				+ column.getTable().getName() + ":" + column.getColumnName()
//				+ ":" + row.equals(column));
//		System.out.println(index + ":" + row.toComString() + " : "
//				+ column.getTable().getName() + ":" + column.getColumnName()
//				+ ":" + row.equals(column));
		if (row instanceof Datum.dLong) {
			accumulator = ((Datum.dLong) row).getValue();

		} else if (row instanceof Datum.dDate) {
			accumulator = ((Datum.dDate) row).getValue();

		} else if (row instanceof Datum.dString) {
			accumulator = ((Datum.dString) row).getValue();

		} else if (row instanceof Datum.dDecimal) {
			accumulator = ((Datum.dDecimal) row).getValue();
		}
		lg.logger.log(Level.INFO, accumulator.toString());
	}

	@Override
	public void visit(AndExpression andExpression) {
		// lg.logger.log(Level.INFO, "Came to and expression");
		andExpression.getLeftExpression().accept(this);
		boolean leftValue = accumulatorBoolean;
		andExpression.getRightExpression().accept(this);
		boolean rightValue = accumulatorBoolean;
		if (leftValue && rightValue) {
			// lg.logger.log(Level.INFO, "TRUE");
			accumulatorBoolean = true;
		} else {
			// lg.logger.log(Level.INFO, "FALSE");
			accumulatorBoolean = false;
		}
	}

	@Override
	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	@Override
	public void visit(Division division) {
		// lg.logger.log(Level.INFO, "Came to addition");
		division.getLeftExpression().accept(this);
		Object leftValue = accumulator;
		division.getRightExpression().accept(this);
		Object rightValue = accumulator;
		if (leftValue instanceof Double && rightValue instanceof Double) {
			accumulator = Double.parseDouble(leftValue.toString())
					/ Double.parseDouble(rightValue.toString());
		} else if (leftValue instanceof Long && rightValue instanceof Long) {
			accumulator = Long.parseLong(leftValue.toString())
					/ Long.parseLong(rightValue.toString());
		}
		// lg.logger.log(Level.INFO,
		// "Division result is"+accumulator.toString());
	}

	@Override
	public void visit(DoubleValue doubleValue) {
		accumulator = doubleValue.getValue();
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		// lg.logger.log(Level.INFO, "Came to greater than");
		accumulatorBoolean = false;
		equalsTo.getLeftExpression().accept(this);
		Object leftValue = accumulator;
		lg.logger.log(Level.INFO, leftValue.getClass().getName());
		equalsTo.getRightExpression().accept(this);
		Object rightValue = accumulator;
		lg.logger.log(Level.INFO, rightValue.getClass().getName());
		if (leftValue instanceof String && rightValue instanceof String) {
			if (leftValue.toString().equals(rightValue.toString())) {
				lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Double && rightValue instanceof Double) {
			if (Double.parseDouble(leftValue.toString()) == Double
					.parseDouble(rightValue.toString())) {
				lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Long && rightValue instanceof Long) {
			if (Long.parseLong(leftValue.toString()) == Long
					.parseLong(rightValue.toString())) {
				lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Date && rightValue instanceof Date) {
			Date date1 = (Date) leftValue;
			Date date2 = (Date) rightValue;
			if (date1.equals(date2)) {
				lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		}

	}

	@Override
	public void visit(Function function) {
//		lg.logger.log(Level.INFO, "Function");
		String functionName = function.getName();
		ExpressionList parameters = function.getParameters();
//		lg.logger.log(Level.INFO, functionName);
//		lg.logger.log(Level.INFO, parameters.toString());
		List expressionList = parameters.getExpressions();
//		lg.logger.log(Level.INFO, expressionList.toString());
		
		
		
		

	}

	@Override
	public void visit(GreaterThan greaterThan) {
		lg.logger.log(Level.INFO, "Came to greater than");
		accumulatorBoolean = false;
		greaterThan.getLeftExpression().accept(this);
		Object leftValue = accumulator;
		lg.logger.log(Level.INFO, "done with left");
//		accumulator=null;
//		System.out.println(greaterThan.getRightExpression().getClass().getName());
		greaterThan.getRightExpression().accept(this);
		Object rightValue = accumulator;
		lg.logger.log(Level.INFO, "done with right");
		lg.logger.log(Level.INFO, "Leftie "+leftValue.getClass().getName()+leftValue.toString());
		lg.logger.log(Level.INFO, "Rightie "+rightValue.getClass().getName()+rightValue.toString());
		if (leftValue instanceof String && rightValue instanceof String) {
			if (leftValue.toString().compareTo(rightValue.toString()) > 0) {
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Double && rightValue instanceof Double) {
			if (Double.parseDouble(leftValue.toString()) > Double
					.parseDouble(rightValue.toString())) {
				 lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Long && rightValue instanceof Long) {
			if (Long.parseLong(leftValue.toString()) > Long
					.parseLong(rightValue.toString())) {
				 lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Date && rightValue instanceof Date) {
			Date date1 = (Date) leftValue;
			Date date2 = (Date) rightValue;
			if (date1.after(date2)) {
				lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		}
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		accumulatorBoolean = false;
		// lg.logger.log(Level.INFO, "Came to greater than equals");
		greaterThanEquals.getLeftExpression().accept(this);
		Object leftValue = accumulator;
		// lg.logger.log(Level.INFO, leftValue.toString());
		// lg.logger.log(Level.INFO, leftValue.getClass().getName());
		greaterThanEquals.getRightExpression().accept(this);
		Object rightValue = accumulator;
		// lg.logger.log(Level.INFO, rightValue.toString());
		// lg.logger.log(Level.INFO, rightValue.getClass().getName());
		if (leftValue instanceof String && rightValue instanceof String) {
			// lg.logger.log(Level.INFO, "String Value");
			if (leftValue.toString().compareTo(rightValue.toString()) >= 0) {
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Double && rightValue instanceof Double) {
			// lg.logger.log(Level.INFO, "Double Value");
			if (Double.parseDouble(leftValue.toString()) >= Double
					.parseDouble(rightValue.toString())) {
				// lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Long && rightValue instanceof Long) {
			// lg.logger.log(Level.INFO, "Long Value");
			if (Long.parseLong(leftValue.toString()) >= Long
					.parseLong(rightValue.toString())) {
				// lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Date && rightValue instanceof Date) {
			// lg.logger.log(Level.INFO, "Date Value");
			Date date1 = (Date) leftValue;
			Date date2 = (Date) rightValue;
			if (date1.after(date2)||date1.equals(date2)) {
				lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		}
	}

	/*
	 * @Override public void visit(InExpression inExpression) {
	 * inExpression.getLeftExpression().accept(this);
	 * inExpression.getItemsList().accept(this); }
	 */

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
		accumulator = longValue.getValue();
		// lg.logger.log(Level.INFO,
		// "Returning long value"+accumulator.toString());
	}

	@Override
	public void visit(Object longValue) {
		accumulator = longValue;
		// lg.logger.log(Level.INFO,
		// "Returning object value"+accumulator.toString());
	}

	@Override
	public void visit(MinorThan minorThan) {
		accumulatorBoolean = false;
		// lg.logger.log(Level.INFO, "Came to greater than equals");
		minorThan.getLeftExpression().accept(this);
		Object leftValue = accumulator;
		minorThan.getRightExpression().accept(this);
		Object rightValue = accumulator;
		if (leftValue instanceof String && rightValue instanceof String) {
			if (leftValue.toString().compareTo(rightValue.toString()) < 0) {
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Double && rightValue instanceof Double) {
			if (Double.parseDouble(leftValue.toString()) < Double
					.parseDouble(rightValue.toString())) {
				// lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Long && rightValue instanceof Long) {
			if (Long.parseLong(leftValue.toString()) < Long
					.parseLong(rightValue.toString())) {
				// lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Date && rightValue instanceof Date) {
			Date date1 = (Date) leftValue;
			Date date2 = (Date) rightValue;
			if (date1.before(date2)) {
				lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		}
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		accumulatorBoolean = false;
		// lg.logger.log(Level.INFO, "Came to minor than equals");
		minorThanEquals.getLeftExpression().accept(this);
		Object leftValue = accumulator;
		minorThanEquals.getRightExpression().accept(this);
		Object rightValue = accumulator;
		if (leftValue instanceof String && rightValue instanceof String) {
			if (leftValue.toString().compareTo(rightValue.toString()) <= 0) {
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Double && rightValue instanceof Double) {
			if (Double.parseDouble(leftValue.toString()) <= Double
					.parseDouble(rightValue.toString())) {
				// lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Long && rightValue instanceof Long) {
			if (Long.parseLong(leftValue.toString()) <= Long
					.parseLong(rightValue.toString())) {
				// lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		} else if (leftValue instanceof Date && rightValue instanceof Date) {
			Date date1 = (Date) leftValue;
			Date date2 = (Date) rightValue;
			if (date1.before(date2)||date1.equals(date2)) {
				lg.logger.log(Level.INFO, "GREATER GREATER");
				accumulatorBoolean = true;
			}
		}
	}

	@Override
	public void visit(Multiplication multiplication) {
		// lg.logger.log(Level.INFO, "Came to multiplication");
		multiplication.getLeftExpression().accept(this);
		Object leftValue = accumulator;
		multiplication.getRightExpression().accept(this);
		Object rightValue = accumulator;
		if (leftValue instanceof Double && rightValue instanceof Double) {
			accumulator = Double.parseDouble(leftValue.toString())
					* Double.parseDouble(rightValue.toString());
		} else if (leftValue instanceof Long && rightValue instanceof Long) {
			accumulator = Long.parseLong(leftValue.toString())
					* Long.parseLong(rightValue.toString());
		}
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
		// lg.logger.log(Level.INFO, "Came to OR expression");
		orExpression.getLeftExpression().accept(this);
		boolean leftValue = accumulatorBoolean;
		orExpression.getRightExpression().accept(this);
		boolean rightValue = accumulatorBoolean;
		if (leftValue || rightValue) {
			// lg.logger.log(Level.INFO, "TRUE");
			accumulatorBoolean = true;
		} else {
			// lg.logger.log(Level.INFO, "FALSE");
			accumulatorBoolean = false;
		}
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue stringValue) {
		lg.logger.log(Level.INFO, "stringgggg");
		accumulator = stringValue.getValue();
	}

	@Override
	public void visit(Subtraction subtraction) {
		// lg.logger.log(Level.INFO, "Came to subtraction");
		subtraction.getLeftExpression().accept(this);
		Object leftValue = accumulator;
		subtraction.getRightExpression().accept(this);
		Object rightValue = accumulator;
		if (leftValue instanceof Double && rightValue instanceof Double) {
			accumulator = Double.parseDouble(leftValue.toString())
					- Double.parseDouble(rightValue.toString());
		} else if (leftValue instanceof Long && rightValue instanceof Long) {
			accumulator = Long.parseLong(leftValue.toString())
					- Long.parseLong(rightValue.toString());
		}
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		// lg.logger.log(Level.INFO, "binary msg");
		// binaryExpression.getRightExpression().accept(this);
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
		lg.logger.log(Level.INFO, "DATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATEDATE");
		accumulator = dateValue.getValue();
	}

	@Override
	public void visit(TimestampValue timestampValue) {
		lg.logger.log(Level.INFO, "TIMESTAMPTIMESTAMPTIMESTAMPTIMESTAMPTIMESTAMPTIMESTAMPTIMESTAMPTIMESTAMPTIMESTAMPTIMESTAMP");
	}

	@Override
	public void visit(TimeValue timeValue) {
		lg.logger.log(Level.INFO, "TIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIMETIME");
	}

	@Override
	public void visit(CaseExpression caseExpression) {
	}

	@Override
	public void visit(WhenClause whenClause) {
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

	public boolean getAccumulatorBoolean() {
		return accumulatorBoolean;
	}

	public void setAccumulatorBoolean(boolean accumulatorBoolean) {
		this.accumulatorBoolean = accumulatorBoolean;
	}
}