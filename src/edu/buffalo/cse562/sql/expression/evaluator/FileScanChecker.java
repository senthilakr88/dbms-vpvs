package edu.buffalo.cse562.sql.expression.evaluator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import edu.buffalo.cse562.physicalPlan.TupleStruct;
import edu.buffalo.cse562.structure.Datum;

public class FileScanChecker extends AbstractExpressionVisitor {
	Column leftColumn;
	Column rightColumn;
	Table leftTable;
	Table rightTable;
	Column column;
	Table table;
	String rightTableS;

	public FileScanChecker(String rightTable) {
//		System.out.println("rightTable :: " + rightTable);
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
		if (rightTableS != null
				&& !rightTableS.equalsIgnoreCase(rightTable.getName())) {
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

	public boolean getFlyTblCheck(
			Map<String, ArrayList<Expression>> indexMapping) {
		List<String> flyTables = TupleStruct.getInFlyTables();
//		System.out.println("TupleStruct.getInFlyTables()  adsa :: "
//				+ TupleStruct.getInFlyTables());
		if (flyTables == null) {
			if (indexMapping.containsKey(leftTable.getName()))
				return true;
			else
				return false;
		}
//		System.out.println("leftTable.getName()  adsa :: "
//				+ leftTable.getName());
		if (TupleStruct.getInFlyTables().contains(leftTable.getName())) {
			return true;
		} else {
			return false;
		}

	}

	public boolean getIndexTblCheck(String indexDir, String tableName) {
		File f;
		f = new File(indexDir + File.separator + rightTableS + ".metadata");
		if (f.exists()) {
			return indexChkHelper(f);
		} else {
			f = new File(indexDir + File.separator + tableName + ".metadata");
			if (f.exists()) {
				return indexChkHelper(f);
			} else {
				return false;
			}
			
		}
	}
	
	public boolean indexChkHelper(File f) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(f));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}

		String indexLine = null, indexFullLine = null;
		Boolean isindex = null;

		try {
			while ((indexFullLine = br.readLine()) != null) {
				// System.out.println("Each index is "+indexFullLine);
				String[] parts = indexFullLine.split("::", 2);
				indexLine = parts[0];
				// System.out.println(indexLine);
				if (!indexLine.contains(",")) {
					// System.out
					// .println("Does not contain comma");
					// System.out
					// .println(lcol.getColumnName());
					if (indexLine.equalsIgnoreCase(rightColumn
							.getColumnName())) {
						return true;
					}
				}
			}
		} catch (IOException e) {

			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return false;
	}
}
