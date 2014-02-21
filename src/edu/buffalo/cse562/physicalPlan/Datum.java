package edu.buffalo.cse562.physicalPlan;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import net.sf.jsqlparser.schema.Column;

public interface Datum {

	public String toString();

	public String toComString();

	public Column getColumn();

	public void setColumn(Column column);

	public boolean equals(Column col);
	
	public class dLong implements Datum {

		Long value;
		Column column;

		public Column getColumn() {
			return column;
		}

		public void setColumn(Column column) {
			this.column = column;
		}

		public dLong(String s, Column col) {
			value = Long.parseLong(s);
			this.column = col;
		}

		public long getValue() {
			return value;
		}

		public void setValue(long value) {
			this.value = value;
		}

		public String toString() {
			return String.valueOf(value);
		}

		public String toComString() {
			return column.getTable().getName() + ":" + column.getTable().getAlias() + ":" + column.getWholeColumnName() + ":" + column.getTable().getAlias() 
					+ ":" + String.valueOf(value) + "\t";
		}

		@Override
		public boolean equals(Column col) {
			if (col == null)
				return false;
//			else if (!column.getTable().getName().equalsIgnoreCase(col.getTable().getName()))
//				return false;
			else if (!column.getColumnName().equalsIgnoreCase(col.getColumnName()))
				return false;
			else
				return true;
		}

		public Datum sumDatum(Datum input){
			Datum sum = null;
			if(input instanceof dLong){
				Long value = this.getValue()+((dLong) input).getValue();
				String valueString = value.toString();
				sum = new dLong(valueString,input.getColumn());
			}
			else if (input instanceof dString){
				
			}
			else if (input instanceof dDate){
				
			}
		return sum;
		}

	}

	public class dString implements Datum {

		String value;
		Column column;

		public dString(String s, Column col) {
			value = s;
			column = col;
		}

		public Column getColumn() {
			return column;
		}

		public void setColumn(Column column) {
			this.column = column;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public String toString() {
			return value;
		}

		public String toComString() {
			return column.getTable().getName() + ":" + column.getTable().getAlias() + ":" + column.getWholeColumnName()
					+ ":" + value + "\t";
		}

		@Override
		public boolean equals(Column col) {
			if (col == null)
				return false;
//			else if (!column.getTable().getName().equalsIgnoreCase(col.getTable().getName()))
//				return false;
			else if (!column.getColumnName().equalsIgnoreCase(col.getColumnName()))
				return false;
			else
				return true;
		}

	}

	public class dDate implements Datum {

		Date value;
		int year;
		int month;
		int day;
		Column column;

		public dDate(String s, Column col) {
//			System.out.println(s);
			
			try {
				value = (new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
						.parse(s));
//				System.out.println(value.toString());
				Calendar cal = Calendar.getInstance();
				cal.setTime(value);
				if (value != null) {
//					year = value.getYear();
					year = cal.get(Calendar.YEAR);

//					month = value.getMonth();
					month = cal.get(Calendar.MONTH)+1;

//					day = value.getDay();
					day = cal.get(Calendar.DAY_OF_MONTH);

				}
				column = col;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public Date getValue() {
			return value;
		}

		public void setValue(Date value) {
			this.value = value;
		}

		public String toString() {
			return String.format("%04d-%02d-%02d", year, month, day);
		}

		public String toComString() {
			return column.getTable().getName() + ":" + column.getTable().getAlias() + ":" + column.getWholeColumnName()
					+ ":" + String.format("%04d-%02d-%02d", year, month, day)
					+ "\t";
		}

		@Override
		public Column getColumn() {
			return column;
		}

		@Override
		public void setColumn(Column column) {
			this.column = column;

		}

		@Override
		public boolean equals(Column col) {
			if (col == null)
				return false;
//			else if (!column.getTable().getName().equalsIgnoreCase(col.getTable().getName()))
//				return false;
			else if (!column.getColumnName().equalsIgnoreCase(col.getColumnName()))
				return false;
			else
				return true;
		}

	}

}
