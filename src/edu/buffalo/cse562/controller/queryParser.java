package edu.buffalo.cse562.controller;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.utilities.fileReader;

public class queryParser {

	List<String> sqlFiles;
	List<String> sqlQueryList;
	logManager lg;
	
	public queryParser(List<String> sqlFiles) {
		// TODO Auto-generated constructor stub
		this.sqlFiles = sqlFiles;
		lg = new logManager();
	}
	
	public void interpretFile() {
		
		Iterator<String> fileIte = sqlFiles.iterator();
		while(fileIte.hasNext()){
			fileReader fr = new fileReader(fileIte.next());
			sqlQueryList = fr.readContents();
			lg.logger.log(Level.INFO, sqlQueryList.toString());
			interpretQuery();
			
		}
	}

	public void interpretQuery() {
		Iterator<String> queryIte = sqlQueryList.iterator();
		while(queryIte.hasNext()){
			CCJSqlParserManager parser = new CCJSqlParserManager();
			Map<String,ArrayList<String>> tableColumnMap = new HashMap<String,ArrayList<String>>();
			try {
				Statement statement = parser.parse(new StringReader(queryIte.next()));
				lg.logger.log(Level.INFO, statement.toString());
				
				/*@author - vino
				 * logic - create a hashMap with table name as the key and array list of column names as value
				 */
				if(statement instanceof CreateTable) {
					lg.logger.log(Level.INFO, "QUERY TYPE: create");
					CreateTable createTableStatement = (CreateTable) statement;
					ArrayList<String> columnNameList = new ArrayList<String>();
					ArrayList<ColumnDefinition> columnDefinitionList = (ArrayList) createTableStatement.getColumnDefinitions();
					for (ColumnDefinition s : columnDefinitionList){
						columnNameList.add(s.getColumnName());
					}
					//Adding table name and column names to the map
					tableColumnMap.put(createTableStatement.getTable().getName(), columnNameList);
					//Printing the contents of the HashMap
					for (Map.Entry<String, ArrayList<String>> entry : tableColumnMap.entrySet()){
						lg.logger.log(Level.INFO, entry.getKey() + "/" + entry.getValue());
					}	
				} else if(statement instanceof Select) {
					
				}
			} catch (JSQLParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
