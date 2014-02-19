package edu.buffalo.cse562.physicalPlan;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import net.sf.jsqlparser.schema.Column;

public interface Datum {

	public String toString();
	public String getColumn();
	public void setColumn(String column);
	
	
	public class dLong implements Datum {

		Long row;
		String column;

		public String getColumn() {
			return column;
		}

		public void setColumn(String column) {
			this.column = column;
		}

		public dLong(String s, String col) {
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

	}
	
	public class dString implements Datum {

		String row;
		String column;

		public dString(String s, String col) {
			row = s;
			column = col;
		}
		
		public String getColumn() {
			return column;
		}

		public void setColumn(String column) {
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

	}
	
	public class dDate implements Datum {

		Date row;
		int year;
		int month;
		int day;
		String column;

		public dDate(String s, String col) {
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
		public String getColumn() {
			return column;
		}

		@Override
		public void setColumn(String column) {
			column = column;
			
		}

	}

}
