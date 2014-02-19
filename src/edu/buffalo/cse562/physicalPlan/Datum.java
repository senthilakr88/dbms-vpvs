package edu.buffalo.cse562.physicalPlan;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import net.sf.jsqlparser.schema.Column;

public interface Datum {

	public String toString();
	public Column getColumn();
	public void setColumn(Column column);
	public boolean equals(Column col);
	
	
	public class dLong implements Datum {

		Long row;
		Column column;

		public Column getColumn() {
			return column;
		}

		public void setColumn(Column column) {
			this.column = column;
		}

		public dLong(String s, Column col) {
			row = Long.parseLong(s);
			this.column = col;
		}
		
		public long getRow() {
			return row;
		}

		public void setRow(long row) {
			this.row = row;
		}
		
		public String toString() {
			return String.valueOf(row);
		}

		@Override
		public boolean equals(Column col) {
			if(col == null)
			return false;
			else if(column.getTable() != col.getTable()) 
				return false;
			else if(column.getColumnName() != col.getColumnName()) 
				return false;
			else 
				return true;
		}

	}
	
	public class dString implements Datum {

		String row;
		Column column;

		public dString(String s, Column col) {
			row = s;
			column = col;
		}
		
		public Column getColumn() {
			return column;
		}

		public void setColumn(Column column) {
			this.column = column;
		}
		
		public String getRow() {
			return row;
		}

		public void setRow(String row) {
			this.row = row;
		}
		
		public String toString() {
			return row;
		}
		
		@Override
		public boolean equals(Column col) {
			if(col == null)
			return false;
			else if(column.getTable() != col.getTable()) 
				return false;
			else if(column.getColumnName() != col.getColumnName()) 
				return false;
			else 
				return true;
		}

	}
	
	public class dDate implements Datum {

		Date row;
		int year;
		int month;
		int day;
		Column column;

		public dDate(String s, Column col) {
			try {
				row = (new SimpleDateFormat(
						"YYYY-MM-DD", Locale.ENGLISH).parse(s));
				if(row!=null){
					year = row.getYear();
					month = row.getMonth();
					day = row.getDay();
				}
				column = col;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public Date getRow() {
			return row;
		}

		public void setRow(Date row) {
			this.row = row;
		}
		
		public String toString() {
			return String.format("%04d-%02d-%02d", year, month, day);
		}

		@Override
		public Column getColumn() {
			return column;
		}

		@Override
		public void setColumn(Column column) {
			column = column;
			
		}
		
		@Override
		public boolean equals(Column col) {
			if(col == null)
			return false;
			else if(column.getTable() != col.getTable()) 
				return false;
			else if(column.getColumnName() != col.getColumnName()) 
				return false;
			else 
				return true;
		}

	}

}
