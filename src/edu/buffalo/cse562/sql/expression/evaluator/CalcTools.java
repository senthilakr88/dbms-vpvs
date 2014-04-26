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

public class CalcTools extends AbstractExpressionVisitor {
	private Datum accumulator;
	private Datum accCount;
	private boolean accumulatorBoolean;
//	private Boolean isExpression;
	private Column columnValue;
	private Boolean firstEntry;
	private Boolean isColumnOnly=null;
	private int columnCount=0;
	Datum[] t;
	List<String> tupleTableMap;
	

	public Datum getResult() {
//		Object accTemp = this.accumulator;
//		this.accumulator = null;
		return accumulator;
	}
	
	public Object getCountResult() {
//		Object accTemp = this.accCount;
//		this.accCount = null;
		return accCount;
	}

	public CalcTools(Datum[] t2) {
		t = t2;
//		isExpression = null;
		firstEntry=null;
		isColumnOnly=null;
		columnCount=0;
	}

	@Override
	public void visit(Addition addition) {
		if(isColumnOnly==null) { isColumnOnly=false;};
		if(firstEntry==null){
			firstEntry=true;
//			System.out.println("Full Expression->"+addition.toString());
			columnValue = new Column(null, addition.toString());
		}
//		isExpression = true;
		addition.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
		addition.getRightExpression().accept(this);
		Datum rightValue = accumulator;
		
		accumulator = leftValue.add(rightValue);
		
//		if (leftValue instanceof Double && rightValue instanceof Double) {
//			accumulator = (Double)leftValue	+ (Double)rightValue;
//			accumulator = (Double)accumulator*10000/10000;
//
//		} else if (leftValue instanceof Long && rightValue instanceof Long) {
//			accumulator = (Long)leftValue + (Long)rightValue;
//		} else if (leftValue instanceof Double && rightValue instanceof Long) {
//			accumulator = (Double)leftValue	+ ((Long)rightValue).doubleValue();
//			accumulator = (Double)accumulator*10000/10000;
//		} else if (leftValue instanceof Long && rightValue instanceof Double) {
//			accumulator = ((Long)leftValue).doubleValue()	+ (Double)rightValue;
//			accumulator = (Double)accumulator*10000/10000;
//		}
	}
	
	public Column getColumn(){
//		if(columnValue!=null){
			return columnValue;
//		} else {
//			return new Column(null,"random");
//		}
	}
	public void visit(Column column) {

		int index=-1;
//		if(isColumnOnly!=null&&isColumnOnly==false) {
//			isColumnOnly=true;
//			System.out.println(isColumnOnly);
//			columnCount+=1;
//			System.out.println(columnCount);
//		}
		
//		System.out.println("Is Expression---->"+isExpression);
//		System.out.println("Came to get column value");
		if(firstEntry==null){
			columnValue = column;
		}
		tupleTableMap = TupleStruct.getTupleTableMap();
		String columnName = column.getWholeColumnName().toLowerCase();
		if(tupleTableMap.contains(columnName)) {
			index = tupleTableMap.indexOf(columnName);
		}
		
//		System.out.println("-------------------------------------------");
//		System.out.println("Visit Column Method");
//		System.out.println(tupleTableMap);
//		System.out.println(columnName);		
//		System.out.println("-------------------------------------------");
//		Datum row = t[index];
		
		accumulator = t[index];
		
		if(isColumnOnly==null) {
			isColumnOnly = true;
		}
		

//		System.out.println(index + " : " + columnName + " : "+tupleTableMap);
		
//		if (row instanceof Datum.dLong) {
//			accumulator = ((Datum.dLong) row).getValue();
//
//		} else if (row instanceof Datum.dDate) {
//			accumulator = ((Datum.dDate) row).getValue();
//
//		} else if (row instanceof Datum.dString) {
//			accumulator = ((Datum.dString) row).getValue();
//
//		} else if (row instanceof Datum.dDecimal) {
//			accumulator = ((Datum.dDecimal) row).getValue();
////			System.out.println(accumulator);
////			System.out.println("ASDF");
////			System.out.println(isColumnOnly);
//			if(isColumnOnly==null)//&&isColumnOnly!=false)
//			{
////				System.out.println("IS Column Only"+columnName);
//				isColumnOnly = true;
////				accumulator = (double)Math.round((double)accumulator * 100) / 100;
//				accumulator = (Double)accumulator*10000/10000;
////				System.out.println(accumulator);
////				accumulator = accumulator;
//			}
////			System.out.println(index + ":" + " : "
////					+ column.getTable().getName() + ":" + column.getColumnName()
////					+ ":" + row.equals(column));
////			System.out.println(accumulator);
//		}
	}

	@Override
	public void visit(AndExpression andExpression) {
//		isExpression = true;
		if(isColumnOnly==null) { isColumnOnly=false;};
		andExpression.getLeftExpression().accept(this);
		boolean leftValue = accumulatorBoolean;
		if(leftValue==false){
			
			return;
		}
		andExpression.getRightExpression().accept(this);
		boolean rightValue = accumulatorBoolean;
		if (leftValue && rightValue) {
			accumulatorBoolean = true;
		} else {
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
		if(isColumnOnly==null) { isColumnOnly=false;};
		if(firstEntry==null){
			firstEntry=true;
//			System.out.println("Full Expression->"+division.toString());
			columnValue = new Column(null, division.toString());
		}
//		isExpression = true;
		division.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
		division.getRightExpression().accept(this);
		Datum rightValue = accumulator;

		accumulator = leftValue.divide(rightValue);
		
//		if (leftValue instanceof Double && rightValue instanceof Double) {
//			accumulator = (Double)leftValue / (Double)rightValue;
//			accumulator = Double.parseDouble(accumulator.toString())*10000/10000;
//
//		} else if (leftValue instanceof Long && rightValue instanceof Long) {
//			accumulator = (Long)leftValue / (Long)rightValue;
//		} else if (leftValue instanceof Double && rightValue instanceof Long) {
//			accumulator = (Double)leftValue	/ ((Long)rightValue).doubleValue();
//			accumulator = (Double)accumulator*10000/10000;
//		} else if (leftValue instanceof Long && rightValue instanceof Double) {
//			accumulator = ((Long)leftValue).doubleValue()	/ (Double)rightValue;
//			accumulator = (Double)accumulator*10000/10000;
//		}
		// "Division result is"+accumulator.toString());
	}

	@Override
	public void visit(DoubleValue doubleValue) {
//		accumulator = doubleValue.getValue()*10000/10000;
		accumulator = new dDecimal(doubleValue.getValue(), null, 4);
	}

	@Override
	public void visit(EqualsTo equalsTo) {
//		isExpression = true;
		if(isColumnOnly==null) { isColumnOnly=false;};
		accumulatorBoolean = false;
		equalsTo.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
//		System.out.println(leftValue.getClass().getName());
		equalsTo.getRightExpression().accept(this);
		Datum rightValue = accumulator;
		
		if(leftValue.compareTo(rightValue) == 0){
			accumulatorBoolean = true;
		}
		
//		System.out.println(rightValue.getClass().getName());
//		if (leftValue instanceof String && rightValue instanceof String) {
//			if (((String)leftValue).compareTo((String)rightValue) == 0) {
//				accumulatorBoolean = true;
////				accumulatorBoolean = ((String)leftValue).equals((String)rightValue);
//			}
//		} else if (leftValue instanceof Double && rightValue instanceof Double) {
//			if (((Double)leftValue).compareTo((Double)rightValue) == 0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Long && rightValue instanceof Long) {
////			System.out.println("Long comparison");
////			System.out.println((Long)leftValue);
////			System.out.println((Long)rightValue);
//			if (((Long)leftValue).compareTo((Long)rightValue) == 0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Date && rightValue instanceof Date) {
//			if (((Date) leftValue).compareTo((Date) rightValue) == 0) {
//				accumulatorBoolean = true;
//			}
//		}
	}
	
	@Override
	public void visit(NotEqualsTo notEqualsTo) {
//		System.out.println("Left EXP---"+notEqualsTo.getLeftExpression());
//		System.out.println("Right EXP---"+notEqualsTo.getRightExpression());
		//System.out.println("INSIDE NOT EQUALS");
//		isExpression = true;
		if(isColumnOnly==null) { isColumnOnly=false;};
		accumulatorBoolean = false;
		notEqualsTo.getLeftExpression().accept(this);
		Datum leftValue = accumulator;

		
		notEqualsTo.getRightExpression().accept(this);
		Datum rightValue = accumulator;
		
		if(leftValue.compareTo(rightValue) != 0){
			accumulatorBoolean = true;
		}
		
//		System.out.println("left val-"+leftValue);
//		System.out.println("right val-"+rightValue);
//		if (leftValue instanceof String && rightValue instanceof String) {
//			//System.out.println("INSTANCE STRING");
////			System.out.println("RIGHT"+rightStringValue);
//			if (((String)leftValue).compareTo((String)rightValue) !=0) {
////				System.out.println("FLAG TRUE");
//				accumulatorBoolean = true;
////			accumulatorBoolean = ((String)leftValue).equals((String)rightValue);
//			}
//		} else if (leftValue instanceof Double && rightValue instanceof Double) {
////			System.out.println("INSTANCE DOUBLE");
//			if (((Double)leftValue).compareTo((Double)rightValue) != 0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Long && rightValue instanceof Long) {
////			System.out.println("INSTANCE LONG");
//			if (((Long)leftValue).compareTo((Long)rightValue) != 0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Date && rightValue instanceof Date) {
////			System.out.println("INSTANCE DATE");
//			if (((Date) leftValue).compareTo((Date) rightValue) != 0) {
//				accumulatorBoolean = true;
//			}
//		}
	}

    @Override                                                                                                                                                             
    public void visit(Function function) {
//    	isExpression = true;
    	if(isColumnOnly==null) { isColumnOnly=false;};
//    	System.out.println("This is a function");
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
        TupleStruct.setTupleTableMap(this.t);
        CalcTools ct = new CalcTools(this.t);

        //Assuming only one date is passed
        //Hence index is 0
        Expression e = (Expression) eList.get(0);
        columnValue = new Column(null, e.toString());
        e.accept(ct);
        this.accumulator = ct.getResult();
    }
    
    public void avg(List eList) {
        TupleStruct.setTupleTableMap(this.t);
        CalcTools ct = new CalcTools(this.t);
        
        //Assuming only one date is passed
        //Hence index is 0
        Expression e = (Expression) eList.get(0);
        columnValue = new Column(null, e.toString());
        e.accept(ct);
        this.accumulator = ct.getResult();
        
    }
    
    public void count(List eList) {
//        System.out.println("entering count");
//        this.accumulator = Long.parseLong(String.valueOf(1));
        this.accumulator = new Datum.dLong(Long.parseLong(String.valueOf(1)), null);
        TupleStruct.setTupleTableMap(this.t);
        CalcTools ct = new CalcTools(this.t);
        Expression e = (Expression) eList.get(0);
//        System.out.println("Expression :: "+eList.toString());
        e.accept(ct);
//        System.out.println("Setting count accum");
        this.accCount = ct.getResult();
//        System.out.println("Setting accum");
        this.accumulator = new Datum.dLong(Long.parseLong(String.valueOf(1)), null);
    }
    
    public void min(List eList) {
        TupleStruct.setTupleTableMap(this.t);
        CalcTools ct = new CalcTools(this.t);
        
        //Assuming only one date is passed
        //Hence index is 0
        Expression e = (Expression) eList.get(0);
        columnValue = new Column(null, e.toString());
        e.accept(ct);
        this.accumulator = ct.getResult();
        
    }
    
    public void max(List eList) {
        TupleStruct.setTupleTableMap(this.t);
        CalcTools ct = new CalcTools(this.t);
        
        //Assuming only one date is passed
        //Hence index is 0
        Expression e = (Expression) eList.get(0);
        columnValue = new Column(null, e.toString());
        e.accept(ct);
        this.accumulator = ct.getResult();
        
    }
    
    public void date(List eList) {
        TupleStruct.setTupleTableMap(this.t);
        CalcTools ct = new CalcTools(this.t);
        Date date = null;

        //Assuming only one date is passed
        //Hence index is 0
        Expression e = (Expression) eList.get(0);
        e.accept(ct);
        this.accumulator = ct.getResult();
//-------------------------------------------------
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//        try {
//			date = dateFormat.parse((String) this.accumulator);
//		} catch (ParseException e1) {
//			
//			e1.printStackTrace();
//		}
//		this.accumulator = date;
//        --------------------------------------------
    }

	@Override
	public void visit(GreaterThan greaterThan) {
		if(isColumnOnly==null) { isColumnOnly=false;};
//		System.out.println("Came into greater than");
//		isExpression = true;
		accumulatorBoolean = false;
		greaterThan.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
//		System.out.println("Done with left"+leftValue.toString());
//		accumulator=null;
//		System.out.println(greaterThan.getRightExpression().getClass().getName());
		greaterThan.getRightExpression().accept(this);
		Datum rightValue = accumulator;
		
		if(leftValue.compareTo(rightValue) >0){
			accumulatorBoolean = true;
		}
//		System.out.println("Done with right"+rightValue.toString());
//		System.out.println("Leftie "+leftValue.getClass().getName()+leftValue.toString());
//		System.out.println("Rightie "+rightValue.getClass().getName()+rightValue.toString());
//--------------------------------------------------------------------------------------------
//		if (leftValue instanceof String && rightValue instanceof String) {
//			if (((String)leftValue).compareTo((String)rightValue) > 0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Double && rightValue instanceof Double) {
//			if (Double.compare((Double)leftValue, (Double)rightValue) >0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Long && rightValue instanceof Long) {
//			if (((Long)leftValue).compareTo((Long)rightValue)>0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Long && rightValue instanceof Double) {
//			if (Double.compare(((Long)leftValue).doubleValue(), (Double)rightValue)>0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Double && rightValue instanceof Long) {
//			if (Double.compare((Double)leftValue, ((Long)rightValue).doubleValue())>0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Date && rightValue instanceof Date) {
//			if (((Date)leftValue).compareTo((Date)rightValue)>0) {
//				accumulatorBoolean = true;
//			}
//		} else {
//		}
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		if(isColumnOnly==null) { isColumnOnly=false;};
		accumulatorBoolean = false;
//		isExpression = true;
		greaterThanEquals.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
		greaterThanEquals.getRightExpression().accept(this);
		Datum rightValue = accumulator;
		
		if(leftValue.compareTo(rightValue) >= 0) {
			accumulatorBoolean = true;
		}
		
		
//		if (leftValue instanceof String && rightValue instanceof String) {
//			if (((String)leftValue).compareTo((String)rightValue) >= 0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Double && rightValue instanceof Double) {
//			if (Double.compare((Double)leftValue, (Double)rightValue)>=0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Long && rightValue instanceof Long) {
//			if (((Long)leftValue).compareTo((Long)rightValue) >=0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Long && rightValue instanceof Double) {
//			if (Double.compare(((Long)leftValue).doubleValue(), (Double)rightValue)>=0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Double && rightValue instanceof Long) {
//			if (Double.compare((Double)leftValue, ((Long)rightValue).doubleValue())>=0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Date && rightValue instanceof Date) {
//			if (((Date) leftValue).compareTo((Date) rightValue)>=0) {
//				accumulatorBoolean = true;
//			}
//		} else {
//		}
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
		if(isColumnOnly==null) { isColumnOnly=false;};
		likeExpression.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
		likeExpression.getRightExpression().accept(this);
		Datum rightValue = accumulator;
//		String rightString = (String) rightValue;
		String rightString = rightValue.getStringValue();
		Pattern pattern = null;
		if(rightString.contains("%")){
			 String patternString = rightString.replace("%", ".*");
			 pattern = Pattern.compile(patternString);
		}
		else if(rightString.contains("_")){
			 String patternString = rightString.replace("_", ".?");
			 pattern = Pattern.compile(patternString);
		}
		if(leftValue instanceof Datum.dString){
			String leftString = leftValue.getStringValue();
			Matcher matcher = pattern.matcher(leftString);
			if(matcher.find()){
			 accumulatorBoolean = true;
			}
		}
		else if(leftValue instanceof Datum.dDecimal){
			Double leftDouble = ((Datum.dDecimal)leftValue).getValue();
			String leftString = String.valueOf(leftDouble);
			Matcher matcher = pattern.matcher(leftString);
			if(matcher.find()){
				accumulatorBoolean = true;
			} 
		}
		else if(leftValue instanceof Datum.dLong){
			Long leftLong = ((Datum.dLong)leftValue).getValue();
			String leftString = String.valueOf(leftLong);
			Matcher matcher = pattern.matcher(leftString);
			if(matcher.find()){
				accumulatorBoolean = true;
			}
		}
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		existsExpression.getRightExpression().accept(this);
	}

	@Override
	public void visit(LongValue longValue) {
		accumulator = new Datum.dLong(longValue.getValue(),null);
		// "Returning long value"+accumulator.toString());
	}

	@Override
	public void visit(Object longValue) {
		accumulator = new Datum.dLong((Long)longValue,null);
		// "Returning object value"+accumulator.toString());
	}

	@Override
	public void visit(MinorThan minorThan) {
		if(isColumnOnly==null) { isColumnOnly=false;};
		accumulatorBoolean = false;
//		isExpression = true;
		minorThan.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
		minorThan.getRightExpression().accept(this);
		Datum rightValue = accumulator;
		
		if(leftValue.compareTo(rightValue) < 0) {
			accumulatorBoolean = true;
		}
		
//		if (leftValue.getClass() == String.class && rightValue.getClass() == String.class) {
//			
//			if (((String)leftValue).compareTo((String)rightValue) < 0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue.getClass() == Double.class && rightValue.getClass() == Double.class) {
//			if (Double.compare((Double)leftValue,  (Double)rightValue) <0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue.getClass() == Long.class && rightValue.getClass() == Long.class) {
//			if (((Long)leftValue).compareTo((Long)rightValue) <0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue.getClass() == Long.class && rightValue.getClass() == Double.class) {
//			if (Double.compare( ((Long)leftValue).doubleValue(), (Double)rightValue)<0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue.getClass() == Double.class && rightValue.getClass() == Long.class) {
//			if (Double.compare((Double)leftValue, ((Long)rightValue).doubleValue())<0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue.getClass() == Date.class && rightValue.getClass() == Date.class) {
//			if (((Date) leftValue).compareTo((Date) rightValue)<0) {
//				accumulatorBoolean = true;
//			}
//		} else {
//		}
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		if(isColumnOnly==null) { isColumnOnly=false;};
		accumulatorBoolean = false;
//		isExpression = true;
		minorThanEquals.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
		minorThanEquals.getRightExpression().accept(this);
		Datum rightValue = accumulator;
		
		if(leftValue.compareTo(rightValue) <= 0) {
			accumulatorBoolean = true;
		}
		
//		if (leftValue instanceof String && rightValue instanceof String) {
//			if (((String)leftValue).compareTo((String)rightValue) <= 0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Double && rightValue instanceof Double) {
//			if (Double.compare((Double)leftValue, (Double)rightValue)<=0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Long && rightValue instanceof Long) {
//			if (((Long)leftValue).compareTo((Long)rightValue) <=0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Long && rightValue instanceof Double) {
//			if (Double.compare(((Long)leftValue).doubleValue(), (Double)rightValue)<=0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Double && rightValue instanceof Long) {
//			if (Double.compare((Double)leftValue, ((Long)rightValue).doubleValue())<=0) {
//				accumulatorBoolean = true;
//			}
//		} else if (leftValue instanceof Date && rightValue instanceof Date) {
//			if (((Date) leftValue).compareTo((Date) rightValue)<=0) {
//				accumulatorBoolean = true;
//			}
//		} else {
//		}
		
	}

	@Override
	public void visit(Multiplication multiplication) {
		if(isColumnOnly==null) { isColumnOnly=false;};
//		isExpression = true;
		if(firstEntry==null){
			firstEntry=true;
//			System.out.println("Full Expression->"+multiplication.toString());
			columnValue = new Column(null, multiplication.toString());
		}
		multiplication.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
		multiplication.getRightExpression().accept(this);
		Datum rightValue = accumulator;
		
		accumulator = leftValue.multiply(rightValue);
		
//		if (leftValue instanceof Double && rightValue instanceof Double) {
//			accumulator = (Double)leftValue
//					* (Double)rightValue;
//			accumulator = Double.parseDouble(accumulator.toString())*10000/10000;
//
//		} else if (leftValue instanceof Long && rightValue instanceof Long) {
//			accumulator = (Long)leftValue
//					* (Long)rightValue;
//		} else if (leftValue instanceof Double && rightValue instanceof Long) {
//			accumulator = (Double)leftValue
//					* ((Long)rightValue).doubleValue();
//			accumulator = (Double)accumulator*10000/10000;
//		} else if (leftValue instanceof Long && rightValue instanceof Double) {
//			accumulator = ((Long)leftValue).doubleValue()
//					* (Double)rightValue;
//			accumulator = (Double)accumulator*10000/10000;
//		}
	}


	@Override
	public void visit(NullValue nullValue) {

	}

	@Override
	public void visit(OrExpression orExpression) {
		if(isColumnOnly==null) { isColumnOnly=false;};
//		isExpression = true;
		orExpression.getLeftExpression().accept(this);
		boolean leftValue = accumulatorBoolean;
		if(leftValue){
//			System.out.println("evaluated true in OR Expression");
			return;
		}
		orExpression.getRightExpression().accept(this);
		boolean rightValue = accumulatorBoolean;
		if (leftValue || rightValue) {
			accumulatorBoolean = true;
		} else {
			accumulatorBoolean = false;
		}
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		if(isColumnOnly==null) { isColumnOnly=false;};
		parenthesis.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue stringValue) {
			accumulator = new dString(stringValue.getValue(),null);
	}

	@Override
	public void visit(Subtraction subtraction) {
		if(isColumnOnly==null) { isColumnOnly=false;};
//		isExpression = true;
		if(firstEntry==null){
			firstEntry=true;
//			System.out.println("Full Expression->"+subtraction.toString());
			columnValue = new Column(null, subtraction.toString());
		}
		subtraction.getLeftExpression().accept(this);
		Datum leftValue = accumulator;
//		System.out.println("subtract Done with left"+leftValue.toString());

		subtraction.getRightExpression().accept(this);
		Datum rightValue = accumulator;
//		System.out.println("subtract Done with right"+rightValue.toString());
//		System.out.println("subtract Leftie "+leftValue.getClass().getName()+leftValue.toString());
//		System.out.println("subtract Rightie "+rightValue.getClass().getName()+rightValue.toString());
		
		accumulator = leftValue.subtract(rightValue);
		
		
//		if (leftValue instanceof Double && rightValue instanceof Double) {
//			accumulator = (Double)leftValue
//					- (Double)rightValue;
////			System.out.println("subtract result 1"+accumulator.getClass().getName());
//			accumulator = (Double)accumulator*10000/10000;
////			System.out.println("subtract result 2"+accumulator.getClass().getName());
//		} else if (leftValue instanceof Long && rightValue instanceof Long) {
//			accumulator = (Long)leftValue
//					- (Long)rightValue;
//		} else if (leftValue instanceof Double && rightValue instanceof Long) {
//			accumulator = (Double)leftValue
//					- ((Long)rightValue).doubleValue();
//			accumulator = (Double)accumulator*10000/10000;
//		} else if (leftValue instanceof Long && rightValue instanceof Double) {
//			accumulator = ((Long)leftValue).doubleValue()
//					- (Double)rightValue;
//			accumulator = (Double)accumulator*10000/10000;
//		}
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
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
		accumulator = new Datum.dDate(dateValue.getValue(),null);
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

	public boolean getAccumulatorBoolean() {
		return accumulatorBoolean;
	}

	public void setAccumulatorBoolean(boolean accumulatorBoolean) {
		this.accumulatorBoolean = accumulatorBoolean;
	}

	public Boolean isColumn() {
		// TODO Auto-generated method stub
		return isColumnOnly;
	}
	
}