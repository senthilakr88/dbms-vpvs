package edu.buffalo.cse562.structure;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import edu.buffalo.cse562.physicalPlan.TupleStruct;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public interface Datum {

	public String toString();
	// public String toComString();
	public Column getColumn();
	public void setColumn(Column col);
	// public void setColumn(Column column);
	public boolean equals(Column col);
	public String getStringValue();
//	public int compareTo(Datum[] tuple);
	public Datum add(Datum rightValue);
	public Datum multiply(Datum rightValue);
	public Datum subtract(Datum rightValue);
	public Datum divide(Datum rightValue);
	public int compareTo(Datum rightValue);
	public void setAliasName(String aliasName);

	
	public class dLong implements Datum, Serializable {

		Long value;
		String columnName;
		String tableName;
		String schemaName;
		String aliasName;

		public void setAliasName(String aliasName) {
			this.aliasName = aliasName;
		}

		public Column getColumn() {
			Table tab = new Table(schemaName, tableName);
			tab.setAlias(aliasName);
			return new Column(tab, columnName);			
		}

		public dLong(dLong d) {
			this(d.value, d.getColumn());
		}

		public dLong(String s, Column col) {
			value = Long.parseLong(s);
			if (col != null) {
				this.columnName = col.getColumnName();
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				}
			}
		}
		
		public dLong(Long s, Column col) {
			value = s;
			if (col != null) {
				this.columnName = col.getColumnName();
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				}
			}
		}
		
		@Override
		public void setColumn(Column col) {
//			System.out.println("---------------------------------------------------------");
//			System.out.println("in set method");
			if (col != null) {
				this.columnName = col.getColumnName();
//				System.out.println("columnName :: " + columnName);
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				} else {
					this.schemaName = null;
					this.tableName = null;
					this.aliasName = null;
				}
//				System.out.println("columnName :: "+columnName);
//				System.out.println("schemaName :: "+schemaName);
//				System.out.println("tableName :: "+tableName);
//				System.out.println("aliasName :: "+aliasName);
			}
//			System.out.println("---------------------------------------------------------");
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

		// public String toComString() {
		// return column.getTable().getName() + ":"
		// + column.getTable().getAlias() + ":"
		// + column.getWholeColumnName() + ":"
		// + column.getTable().getAlias() + ":"
		// + String.valueOf(value) + "\t";
		// }

		public int hashCode() {
			long hash = 7;
			hash = (31 * hash) + value;
			hash = (31 * hash) + (null == value ? 0 : value.hashCode());
			return (int) hash;
		}

		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if ((obj == null) || (obj.getClass() != this.getClass()))
				return false;
			// object must be Test at this point
			dLong test = (dLong) obj;
			return (this.value == test.value) ? true : false;
		}

		@Override
		public boolean equals(Column col) {
			if (col == null)
				return false;
			// else if
			// (!column.getTable().getName().equalsIgnoreCase(col.getTable().getName()))
			// return false;
			else if (!columnName.equalsIgnoreCase(col.getColumnName()))
				return false;
			else
				return true;
		}

		public Datum sumDatum(Datum input) {
			Datum sum = null;
			if (input instanceof dLong) {
				long arg1 = (long) this.getValue();
				long arg2 = (long) ((dLong) input).getValue();
				Long value = arg1 + arg2;
				String valueString = value.toString();
				sum = new dLong(valueString, input.getColumn());
			} else if (input instanceof dString) {

			} else if (input instanceof dDate) {

			}
			return sum;
		}

		@Override
		public String getStringValue() {
			return value.toString();
		}

		@Override
		public Datum add(Datum rightValue) {
			if(rightValue instanceof dDecimal){
				Double value = this.value+((dDecimal)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value, null,4);
				
			} else {
				return new dLong((this.value+ ((dLong)rightValue).getValue()), null);
			}
			
		}

		@Override
		public Datum divide(Datum rightValue) {
			if(rightValue.getClass() == dDecimal.class){
				Double value = this.value/((dDecimal)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value,null,4);
				
			} else {
				return new dLong((this.value/ ((dLong)rightValue).getValue()), null);
			}
		}

		@Override
		public int compareTo(Datum rightValue) {
			if(rightValue.getClass() == dLong.class){
				return this.value.compareTo(((dLong)rightValue).getValue());
			} else {
				Long value = Long.parseLong(((dDecimal)rightValue).getStringValue());
				return this.value.compareTo(value);
			}
		}

		@Override
		public Datum multiply(Datum rightValue) {
			if(rightValue.getClass() == dDecimal.class){
				Double value = this.value*((dDecimal)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value, null,4);
				
			} else {
				return new dLong((this.value * ((dLong)rightValue).getValue()), null);
			}
		}
		
		public Column compCol(Column rightCol) {
			if(rightCol != null) {
				return rightCol;
			} else if(this.columnName != null) {
				return getColumn();
			} else {
				return null;
			}
		}
		

		@Override
		public Datum subtract(Datum rightValue) {
			if(rightValue.getClass() == dDecimal.class){
				Double value = this.value-((dDecimal)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value, null,4);
				
			} else {
				return new dLong((this.value - ((dLong)rightValue).getValue()), null);
			}
		}
	}

	public class dDecimal implements Datum, Serializable {

		Double value;
		String columnName;
		String tableName;
		String schemaName;
		String aliasName;
		int precision;

		public Column getColumn() {
			Table tab = new Table(schemaName, tableName);
			tab.setAlias(aliasName);
			return new Column(tab, columnName);		
		}
		
		public void setAliasName(String aliasName) {
			this.aliasName = aliasName;
		}
		
		@Override
		public void setColumn(Column col) {
//			System.out.println("---------------------------------------------------------");
//			System.out.println("in set method");
			if (col != null) {
				this.columnName = col.getColumnName();
//				System.out.println("columnName :: " + columnName);
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				} else {
					this.schemaName = null;
					this.tableName = null;
					this.aliasName = null;
				}
//				System.out.println("columnName :: "+columnName);
//				System.out.println("schemaName :: "+schemaName);
//				System.out.println("tableName :: "+tableName);
//				System.out.println("aliasName :: "+aliasName);
			}
//			System.out.println("---------------------------------------------------------");
			
		}

		public dDecimal(dDecimal d) {
			this(d.value, d.getColumn(), d.precision);
		}
		
		public dDecimal(String s, Column col, int prec) {
			value = Double.parseDouble(s);
			precision = prec;
//			System.out.println("Precision is "+precision+" for column ");
			if (col != null) {
				this.columnName = col.getColumnName();
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				}
			}
		}

		public dDecimal(Double s, Column col, int prec) {
			value = s;
			precision = prec;
			if (col != null) {
				this.columnName = col.getColumnName();
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				}
			}
		}

		public dDecimal(Integer s, Column col) {
			value = Double.parseDouble(s.toString());
			if (col != null) {
				this.columnName = col.getColumnName();
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				}
			}
		}

		public Double getValue() {
			return value;
		}

		public void setValue(Double value) {
			this.value = value;
		}

		public String toString() {
			if(precision==2){
				return String.format("%.2f", value);
			} else {
				return String.format("%.4f", value);	
			}
			
		}

		// public String toComString() {
		// return column.getTable().getName() + ":"
		// + column.getTable().getAlias() + ":"
		// + column.getWholeColumnName() + ":"
		// + column.getTable().getAlias() + ":"
		// + String.valueOf(value) + "\t";
		// }

		@Override
		public boolean equals(Column col) {
			if (col == null)
				return false;
			// else if
			// (!column.getTable().getName().equalsIgnoreCase(col.getTable().getName()))
			// return false;
			else if (!columnName.equalsIgnoreCase(col.getColumnName()))
				return false;
			else
				return true;
		}

		public Datum sumDatum(Datum input) {
			Datum sum = null;
			if (input instanceof dDecimal) {
				Double value = this.getValue() + ((dDecimal) input).getValue();
				String valueString = value.toString();
				sum = new dLong(valueString, input.getColumn());
			} else if (input instanceof dString) {

			} else if (input instanceof dDate) {

			}
			return sum;
		}

		@Override
		public String getStringValue() {
			return value.toString();
		}

		@Override
		public Datum add(Datum rightValue) {
			if(rightValue.getClass() == dDecimal.class){
				Double value = this.value+((dDecimal)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value,null,4);
				
			} else {
				Double value = this.value+((dLong)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value,null,4);
			}
		}

		@Override
		public Datum divide(Datum rightValue) {
			if(rightValue.getClass() == dDecimal.class){
				Double value = this.value/((dDecimal)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value,null,4);
				
			} else {
				Double value = this.value/((dLong)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value,null,4);
			}
		}

		@Override
		public int compareTo(Datum rightValue) {
			if(rightValue.getClass() == dDecimal.class){
				return this.value.compareTo(((dDecimal)rightValue).getValue());
			} else {
				Double value = (double) ((dLong)rightValue).getValue();
				return this.value.compareTo(value);
			}
		}
		
		public Column compCol(Column rightCol) {
			if(rightCol != null) {
				return rightCol;
			} else if(this.columnName != null) {
				return getColumn();
			} else {
				return null;
			}
		}
		

		@Override
		public Datum subtract(Datum rightValue) {
			if(rightValue.getClass() == dDecimal.class){
				Double value = this.value-((dDecimal)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value,null,4);
				
			} else {
				Double value = this.value-((dLong)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value,null,4);
			}
		}

		@Override
		public Datum multiply(Datum rightValue) {
			if(rightValue.getClass() == dDecimal.class){
				Double value = this.value*((dDecimal)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value,null,4);
				
			} else {
				Double value = this.value*((dLong)rightValue).getValue();
				value = (value * 1000)/1000;
				return new dDecimal(value,null,4);
			}
		}

		public void setPrecision(int i) {
			this.precision = i;
		}
		
	}

	public class dString implements Datum, Serializable {

		String value;
		String columnName;
		String tableName;
		String schemaName;
		String aliasName;

		public dString(String s, Column col) {
			value = s;
			if (col != null) {
				this.columnName = col.getColumnName();
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				}
			}
		}
		
		public dString(dString d) {
			this(d.value, d.getColumn());
		}
		
		public void setAliasName(String aliasName) {
			this.aliasName = aliasName;
		}
		
		public Column getColumn() {
			Table tab = new Table(schemaName, tableName);
			tab.setAlias(aliasName);
			return new Column(tab, columnName);		
		}
		
		@Override
		public void setColumn(Column col) {
//			System.out.println("---------------------------------------------------------");
//			System.out.println("in set method");
			if (col != null) {
				this.columnName = col.getColumnName();
//				System.out.println("columnName :: " + columnName);
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				} else {
					this.schemaName = null;
					this.tableName = null;
					this.aliasName = null;
				}
//				System.out.println("columnName :: "+columnName);
//				System.out.println("schemaName :: "+schemaName);
//				System.out.println("tableName :: "+tableName);
//				System.out.println("aliasName :: "+aliasName);
			}
//			System.out.println("---------------------------------------------------------");
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

		// public String toComString() {
		// return column.getTable().getName() + ":"
		// + column.getTable().getAlias() + ":"
		// + column.getWholeColumnName() + ":" + value + "\t";
		// }

		@Override
		public boolean equals(Column col) {
			if (col == null)
				return false;
			// else if
			// (!column.getTable().getName().equalsIgnoreCase(col.getTable().getName()))
			// return false;
			else if (!columnName.equalsIgnoreCase(col.getColumnName()))
				return false;
			else
				return true;
		}

		public int hashCode() {
			String hash = "hash";
			int ret;
			hash = hash.concat(value);
			ret = hash.hashCode() + (null == value ? 0 : value.hashCode());
			return ret;
		}

		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if ((obj == null) || (obj.getClass() != this.getClass()))
				return false;
			dString test = (dString) obj;
			return (this.value.equals(test.value)) ? true : false;
		}

		@Override
		public String getStringValue() {
			return value.toString();
		}

		void readObjectNoData() throws ObjectStreamException {
			throw new InvalidObjectException("Stream data required");
		}

		@Override
		public Datum add(Datum rightValue) {
			return new dString(this.value+rightValue.getStringValue(),null);
		}

		@Override
		public Datum divide(Datum rightValue) {
			System.out.println("Divide not implemented for String :: Datum Class");
			return null;
		}

		@Override
		public int compareTo(Datum rightValue) {
//			System.out.println("value "+this.value);
			return this.value.compareTo(((dString)rightValue).getValue());
		}

		@Override
		public Datum multiply(Datum rightValue) {
			System.out.println("Multiply not implemented for String :: Datum Class");
			return null;
		}

		@Override
		public Datum subtract(Datum rightValue) {
			System.out.println("Subtract not implemented for String :: Datum Class");
			return null;
		}
		
	}

	public class dDate implements Datum, Serializable {

		Date value;
//		int year;
//		int month;
//		int day;
		String columnName;
		String tableName;
		String schemaName;
		String aliasName;

		public dDate(String s, Column col) {
			// System.out.println(s);

			try {
				value = (new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
						.parse(s));
				// System.out.println(value.toString());
				Calendar cal = Calendar.getInstance();
				cal.setTime(value);
//				if (value != null) {
//					// year = value.getYear();
//					year = cal.get(Calendar.YEAR);
//
//					// month = value.getMonth();
//					month = cal.get(Calendar.MONTH) + 1;
//
//					// day = value.getDay();
//					day = cal.get(Calendar.DAY_OF_MONTH);
//
//				}
				if (col != null) {
					this.columnName = col.getColumnName();
					if (col.getTable() != null) {
						this.schemaName = col.getTable().getSchemaName();
						this.tableName = col.getTable().getName();
						this.aliasName = col.getTable().getAlias();
					}
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public dDate(dDate d) {
			this(d.value, d.getColumn());
		}
		
		public void setAliasName(String aliasName) {
			this.aliasName = aliasName;
		}
		
		public dDate(Date s, Column col) {
			value = s;
			if (col != null) {
				this.columnName = col.getColumnName();
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				}
			}
		}

		@Override
		public void setColumn(Column col) {
//			System.out.println("---------------------------------------------------------");
//			System.out.println("in set method");
			if (col != null) {
				this.columnName = col.getColumnName();
//				System.out.println("columnName :: " + columnName);
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				} else {
					this.schemaName = null;
					this.tableName = null;
					this.aliasName = null;
				}
//				System.out.println("columnName :: "+columnName);
//				System.out.println("schemaName :: "+schemaName);
//				System.out.println("tableName :: "+tableName);
//				System.out.println("aliasName :: "+aliasName);
			}
//			System.out.println("---------------------------------------------------------");			
		}
		
		public Date getValue() {
			return value;
		}

		public String toString() {
			SimpleDateFormat ft = 
				      new SimpleDateFormat ("yyyy-MM-dd");
//			return String.format("%04d-%02d-%02d", year, month, day);
			return ft.format(value);
		}

		// public String toComString() {
		// return column.getTable().getName() + ":"
		// + column.getTable().getAlias() + ":"
		// + column.getWholeColumnName() + ":"
		// + String.format("%04d-%02d-%02d", year, month, day) + "\t";
		// }

		@Override
		public Column getColumn() {
			Table tab = new Table(schemaName, tableName);
			tab.setAlias(aliasName);
			return new Column(tab, columnName);		
		}

		@Override
		public boolean equals(Column col) {
			if (col == null)
				return false;
			// else if
			// (!column.getTable().getName().equalsIgnoreCase(col.getTable().getName()))
			// return false;
			else if (!columnName.equalsIgnoreCase(col.getColumnName()))
				return false;
			else
				return true;
		}

		@Override
		public String getStringValue() {
			SimpleDateFormat ft = 
				      new SimpleDateFormat ("yyyy-MM-dd");
//			return String.format("%04d-%02d-%02d", year, month, day);
			return ft.format(value);
		}

		@Override
		public Datum add(Datum rightValue) {
			System.out.println("Add not implemented for Date :: Datum Class");
			return null;
		}
		
		@Override
		public Datum divide(Datum rightValue) {
			System.out.println("Divide not implemented for Date :: Datum Class");
			return null;
		}

		@Override
		public int compareTo(Datum rightValue) {
//			System.out.println(this.value + " :: " + rightValue.getStringValue());
			if(rightValue instanceof dString) {
				return this.value.compareTo(new dDate(((dString)rightValue).getValue(),null).getValue());
			} else {
				return this.value.compareTo(((dDate)rightValue).getValue());
			}
			
		}

		@Override
		public Datum multiply(Datum rightValue) {
			System.out.println("Multiply not implemented for Date :: Datum Class");
			return null;
		}

		@Override
		public Datum subtract(Datum rightValue) {
			System.out.println("Subtract not implemented for Date :: Datum Class");
			return null;
		}
		
	}
	
	public class Row implements Serializable, Comparable<Row> {

		private static final long serialVersionUID = 1L;
		public Datum[] data;

		public Row(Datum[] data) {
			this.data = data;
		}

		@Override
		public int compareTo(Row o) {
			return TupleStruct.getCompareValue(this.data, o.data);
		}
		
		public Datum[] getDatum() {
			return data;
		}
		
		public String toString() {
			StringBuilder str = new StringBuilder();
			Boolean first = true;
			if (data != null && data.length != 0) {
				for (Datum col : data) {
					if (!first)
						str.append("|" + col);
					else {
						str.append(col);
						first = false;
					}
				}
				str.append("\n");
			}
			return str.toString();
		}

	}

}
