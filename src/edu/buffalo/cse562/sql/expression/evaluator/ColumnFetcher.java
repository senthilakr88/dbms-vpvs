package edu.buffalo.cse562.sql.expression.evaluator;

import java.util.List;

import edu.buffalo.cse562.physicalPlan.Datum;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class ColumnFetcher extends AbstractExpressionVisitor {

	Column leftColumn;
	Column rightColumn;
	Table leftTable;
	Table rightTable;
	Column column;
	Table table;
	Datum[] tempTuple;
	String rightTableS;
	
	public ColumnFetcher() {
		
	}
	
	
	
	public ColumnFetcher(String rightTable) {
		this.rightTableS = rightTable;
	}



	public Column getLeftCol() {
		return leftColumn;
	}
	
	public Column getRightCol() {
		return rightColumn;
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
	
	public void visit(Column column) {
		this.column = column;
	}
	
	public void visit(EqualsTo equalsTo) {
		equalsTo.getLeftExpression().accept(this);
		leftColumn = column;
		leftTable = column.getTable();
		equalsTo.getRightExpression().accept(this);
		rightColumn = column;
		rightTable = column.getTable();
//		TupleStruct.setTupleTableMap(tempTuple);
//		System.out.println("Equals :: "+TupleStruct.getColIndex(tempTuple, column));
//		System.out.println(rightTableS);
//		System.out.println(rightTable.getName());
		if(rightTableS != null && !rightTableS.equalsIgnoreCase(rightTable.getName())) {
//			System.out.println("Entering HH");
			Column tempColumn = leftColumn;
			Table tempTable = leftTable;
			leftColumn = rightColumn;
			leftTable = rightTable;
			rightColumn = tempColumn;
			rightTable = tempTable;
		}
			
	}
	
	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}
}
