package edu.buffalo.cse562.controller;

import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
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
			try {
				Statement statement = parser.parse(new StringReader(queryIte.next()));
				lg.logger.log(Level.INFO, statement.toString());
				if(statement instanceof CreateTable) {
					
				} else if(statement instanceof Select) {
					
				}
			} catch (JSQLParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
