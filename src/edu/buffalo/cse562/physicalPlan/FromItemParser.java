package edu.buffalo.cse562.physicalPlan;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import edu.buffalo.cse562.logicalPlan.components;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromItemParser implements FromItemVisitor {

	Operator oper = null;
	String basePath = null;
	List<Column> tableMap;
	String tableName;
	String swapDir;
	Map<String, ArrayList<String>> tableColTypeMap;
	StringBuffer planPrint;

	public FromItemParser(String basePath, List<Column> tableMap,
			Map<String, ArrayList<String>> tableColTypeMap, String swapDir) {
		this.basePath = basePath;
		this.tableMap = tableMap;
		this.tableColTypeMap = tableColTypeMap;
		this.planPrint = new StringBuffer();
		this.swapDir = swapDir;
	}

	public void addToPlan(String s) {
		s += planPrint.toString() + " ";
		planPrint = new StringBuffer(s);
	}
	
	public StringBuffer getPlan(){
		return planPrint;
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
		oper = new FileScanOperator(table, basePath, tableMap, tableColTypeMap);
		addToPlan("[File Scan on :: "+ table+"]");
	}
	
	@Override
	public void visit(SubSelect subSelect) {
//		System.out.println("Came to subselect");
		addToPlan("subSelect on "+ subSelect);
		SelectBody selectStmt = subSelect.getSelectBody();
		if (selectStmt instanceof PlainSelect) {
			PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
			//System.out.println(plainSelect.toString());
			components comp = new components();
			comp.initializeParam();
			comp.addColsToTable((ArrayList<Column>) tableMap);
			comp.addColsTypeToTable(tableColTypeMap);
			comp.setTableDirectory(basePath);
			comp.setSwapDirectory(swapDir);
			comp.addProjectStmts(plainSelect.getSelectItems());
			comp.setSelectBody(selectStmt);
			comp.setFromItems(plainSelect.getFromItem());
			comp.addWhereConditions(plainSelect.getWhere());
			comp.addOrderBy(plainSelect.getOrderByElements());
			comp.addJoins(plainSelect.getJoins());
			comp.addFileSize(fileSizeComp(plainSelect.getJoins()));
			TupleStruct.setNestedCondition(true);
//			printPlan();
			oper = comp.executePhysicalPlan();
			
			
		} else {
			System.out.println("This subselect statement is yet to be handled :: "+selectStmt);
		}
	}
	
	public void printPlan() {
		System.out.println();
		System.out.println("-------------------------------------------------");
		System.out.println(planPrint.toString());
		System.out.println("-------------------------------------------------");
		System.out.println();
	}

	@Override
	public void visit(SubJoin subJoin) {
		addToPlan("subJoin unimplemented on "+ subJoin);
	}

	public Operator getOperator() {
		return oper;
	}
	
	public String getOperatorTableName() {
		return tableName;
	}

	private Long fileSizeComp(List joins) {
		boolean first = true;
		Long maxSize = null;
		String basePath1 = basePath + File.separator;
		if (!(new File(basePath1).exists())) {
			basePath1 = new File("").getAbsolutePath() + File.separator
					+ basePath1;
		} 
		if(joins!=null){
			Iterator JoinIte = joins.iterator();
//			System.out.println(joins);
			while(JoinIte.hasNext()) {
				Join j = (Join) JoinIte.next();
				if(j.getRightItem() instanceof Table) {
					Table t =  (Table) j.getRightItem();
					String tableName = t.getName();
					File f = new File(basePath1+File.separator+tableName+".dat");
					if(first) {
						maxSize = f.length();
						first = false;
					} else if(maxSize.compareTo(f.length()) < 0) {
						maxSize = f.length();
					}
//					System.out.println("From Item Parser");
//					System.out.println(tableName + " :: " + f.length() + "::" + maxSize);
				}
			}
		} else {
			maxSize = Long.valueOf(1000000);
		}
		return maxSize;
	}
}
