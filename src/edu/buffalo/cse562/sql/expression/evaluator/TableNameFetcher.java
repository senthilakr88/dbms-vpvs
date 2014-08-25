package edu.buffalo.cse562.sql.expression.evaluator;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import edu.buffalo.cse562.logicalPlan.components;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class TableNameFetcher implements FromItemVisitor {

	String tableName;
	public TableNameFetcher() {
		
	}
	
	@Override
	public void visit(Table table) {
//		System.out.println("Came to table");
		if(table.getAlias() != null) {
//			System.out.println("getting alias");
			tableName = table.getAlias();
		} else {
			tableName = table.getWholeTableName();
		}
	}
	
	@Override
	public void visit(SubSelect subSelect) {
		//pass
	}
	
	@Override
	public void visit(SubJoin subJoin) {
		//pass
	}
	
	public String getTableName() {
		return tableName;
	}
}
