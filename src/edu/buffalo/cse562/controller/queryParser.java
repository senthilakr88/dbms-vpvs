package edu.buffalo.cse562.controller;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.logicalPlan.buildindexer;
import edu.buffalo.cse562.logicalPlan.components;
import edu.buffalo.cse562.physicalPlan.Operator;
import edu.buffalo.cse562.utilities.fileReader;

public class queryParser {

	List<String> sqlFiles;
	List<String> sqlQueryList;
	components comp;
	buildindexer bi;
	String tableDir;
	String swapDir;
	String indexDir;
	String preCompDir;
	boolean isBuild;

	public queryParser(String tableDir, String swapDir, List<String> sqlFiles, String indexDir, String preCompDir, boolean isBuild) {
		this.sqlFiles = sqlFiles;
		this.tableDir = tableDir;
		this.swapDir = swapDir;
		this.comp = new components();
		this.indexDir = indexDir;
		this.preCompDir = preCompDir;
		this.isBuild = isBuild;
		this.bi = new buildindexer(tableDir, indexDir, preCompDir);
	}

	public void interpretFile() {

		Iterator<String> fileIte = sqlFiles.iterator();
		while (fileIte.hasNext()) {
			fileReader fr = new fileReader(fileIte.next());
			sqlQueryList = fr.readContents();
			//			printQuery();
			//			comp = new components();
			interpretQuery();

		}
	}

	public void printQuery() {
		Iterator<String> queryIte = sqlQueryList.iterator();
		while (queryIte.hasNext()) {
			System.out.println(queryIte.next());
		}
	}

	public void interpretQuery() {
		Iterator<String> queryIte = sqlQueryList.iterator();
		while (queryIte.hasNext()) {
			CCJSqlParserManager parser = new CCJSqlParserManager();

			try {
				Statement statement = parser.parse(new StringReader(queryIte
						.next()));

				/*
				 * @author - vino logic - create a hashMap with table name as
				 * the key and array list of column names as value
				 */
				if(isBuild) {
					if (statement instanceof CreateTable) {
//						System.out.println("Entering build part of create statement");
						bi.parseStatement((CreateTable) statement);
					}
				} else {
					 if (statement instanceof Select) {
						comp.initializeParam();
						SelectBody selectStmt = ((Select) statement)
								.getSelectBody();
						if (selectStmt instanceof PlainSelect) {

							PlainSelect plainSelect = (PlainSelect) selectStmt;
							comp.addProjectStmts(plainSelect.getSelectItems());
							comp.setSelectBody(selectStmt);
							// plainSelect.getFromItem().toString());
							comp.setFromItems(plainSelect.getFromItem());
							comp.addWhereConditions(plainSelect.getWhere());
							comp.addOrderBy(plainSelect.getOrderByElements());
							comp.addJoins(plainSelect.getJoins());
							comp.addFileSize(fileSizeComp(plainSelect.getJoins()));
							comp.addLimit(plainSelect.getLimit());
							comp.setSql(plainSelect.toString());
							
							Operator oper = comp.executePhysicalPlan();
							if(oper!=null)
								comp.processTuples(oper);
							comp.resetParam();
						} else {
							System.out
							.println("Select type of statement !!! still not handled");
						}

					} else {
						System.out
						.println("Not a create or select statement !!! Skipped from validation");
					}
				}
				
			} catch (JSQLParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private Long fileSizeComp(List joins) {
		boolean first = true;
		Long maxSize = null;
		String basePath = tableDir + File.separator;
		if (!(new File(basePath).exists())) {
			basePath = new File("").getAbsolutePath() + File.separator
					+ basePath;
		} 
		if(joins!=null){
			Iterator JoinIte = joins.iterator();
			while(JoinIte.hasNext()) {
				Join j = (Join) JoinIte.next();
				if(j.getRightItem() instanceof Table) {
					Table t =  (Table) j.getRightItem();
					String tableName = t.getName();
					File f = new File(basePath+File.separator+tableName+".dat");
					if(first) {
						maxSize = f.length();
						first = false;
					} else if(maxSize.compareTo(f.length()) < 0) {
						maxSize = f.length();
					}
//					System.out.println(tableName + " :: " + f.length() + "::" + maxSize);
				}
			}
		} else {
			maxSize = Long.valueOf(1000000);
		}
		return maxSize;
	}

}
