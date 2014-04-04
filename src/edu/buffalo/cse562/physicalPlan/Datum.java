package edu.buffalo.cse562.physicalPlan;

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

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public interface Datum {

	public String toString();

	// public String toComString();

	public Column getColumn();

	// public void setColumn(Column column);

	public boolean equals(Column col);

	public String getStringValue();

	public int compareTo(Datum[] tuple);

	// void writeObject(final ObjectOutputStream out);

	// void readObject(final ObjectInputStream in);

	public class dLong implements Datum, Serializable {

		Long value;
		String columnName;
		String tableName;
		String schemaName;
		String aliasName;

		public Column getColumn() {
			return new Column(new Table(schemaName, tableName), columnName);
		}

		// public void setColumn(Column column) {
		// this.column = column;
		// }

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
		public int compareTo(Datum[] tuple) {
			int index = TupleStruct.getColIndex(tuple, getColumn());
			Object key = TupleStruct.getKey(tuple, index);
			return value.compareTo((Long) key);
		}

		// @Override
		// public void writeObject(ObjectOutputStream out) {
		// try {
		// out.writeLong(this.value);
		// out.writeUTF(column.getTable().getSchemaName());
		// out.writeUTF(column.getTable().getName());
		// out.writeUTF(column.getColumnName());
		// } catch (IOException e) {
		//
		// e.printStackTrace();
		// }
		//
		// }
		//
		// @Override
		// public void readObject(ObjectInputStream in) {
		// try {
		// this.value = in.readLong();
		// this.column = new Column(new Table(in.readUTF(), in.readUTF()),
		// in.readUTF());
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		//
		// }

	}

	public class dDecimal implements Datum, Serializable {

		Double value;
		String columnName;
		String tableName;
		String schemaName;
		String aliasName;

		public Column getColumn() {
			return new Column(new Table(schemaName, tableName), columnName);
		}

		// public void setColumn(Column column) {
		// this.column = column;
		// }

		public dDecimal(String s, Column col) {
			value = Double.parseDouble(s);
			if (col != null) {
				this.columnName = col.getColumnName();
				if (col.getTable() != null) {
					this.schemaName = col.getTable().getSchemaName();
					this.tableName = col.getTable().getName();
					this.aliasName = col.getTable().getAlias();
				}
			}
		}

		public dDecimal(Double s, Column col) {
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
			return String.format("%.2f", value);
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
		public int compareTo(Datum[] tuple) {
			int index = TupleStruct.getColIndex(tuple, getColumn());
			Object key = TupleStruct.getKey(tuple, index);
			return value.compareTo((Double) key);
		}

		// @Override
		// public void writeObject(ObjectOutputStream out) {
		// try {
		// out.writeDouble(this.value);
		// out.writeUTF(column.getTable().getSchemaName());
		// out.writeUTF(column.getTable().getName());
		// out.writeUTF(column.getColumnName());
		// } catch (IOException e) {
		//
		// e.printStackTrace();
		// }
		//
		// }
		//
		// @Override
		// public void readObject(ObjectInputStream in) {
		// try {
		// this.value = in.readDouble();
		// this.column = new Column(new Table(in.readUTF(), in.readUTF()),
		// in.readUTF());
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		//
		// }

	}

	public class dString implements Datum, Serializable {

		private String value;
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

		public Column getColumn() {
			return new Column(new Table(schemaName, tableName), columnName);
		}

		// public void setColumn(Column column) {
		// this.column = column;
		// }

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

		@Override
		public int compareTo(Datum[] tuple) {
			int index = TupleStruct.getColIndex(tuple, getColumn());
			Object key = TupleStruct.getKey(tuple, index);
			return value.compareTo((String) key);
		}

		// @Override
		// public void writeObject(ObjectOutputStream out) {
		// try {
		// out.writeUTF(this.value);
		// out.writeUTF(column.getTable().getSchemaName());
		// out.writeUTF(column.getTable().getName());
		// out.writeUTF(column.getColumnName());
		// } catch (IOException e) {
		//
		// e.printStackTrace();
		// }
		//
		// }
		//
		//
		// public void readObject(ObjectInputStream in) {
		// try {
		// this.value = in.readUTF();
		// this.column = new Column(new Table(in.readUTF(), in.readUTF()),
		// in.readUTF());
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		//
		// }

		void readObjectNoData() throws ObjectStreamException {
			throw new InvalidObjectException("Stream data required");
		}
	}

	public class dDate implements Datum {

		Date value;
		int year;
		int month;
		int day;
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
				if (value != null) {
					// year = value.getYear();
					year = cal.get(Calendar.YEAR);

					// month = value.getMonth();
					month = cal.get(Calendar.MONTH) + 1;

					// day = value.getDay();
					day = cal.get(Calendar.DAY_OF_MONTH);

				}
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

		public Date getValue() {
			return value;
		}

		// public void setValue(Date value) {
		// this.value = value;
		// }

		public String toString() {
			return String.format("%04d-%02d-%02d", year, month, day);
		}

		// public String toComString() {
		// return column.getTable().getName() + ":"
		// + column.getTable().getAlias() + ":"
		// + column.getWholeColumnName() + ":"
		// + String.format("%04d-%02d-%02d", year, month, day) + "\t";
		// }

		@Override
		public Column getColumn() {
			return new Column(new Table(schemaName, tableName), columnName);
		}

		// @Override
		// public void setColumn(Column column) {
		// this.column = column;
		//
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

		@Override
		public String getStringValue() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int compareTo(Datum[] tuple) {
			int index = TupleStruct.getColIndex(tuple, getColumn());
			Object key = TupleStruct.getKey(tuple, index);
			return value.compareTo((Date) key);
		}

		// @Override
		// public void writeObject(ObjectOutputStream out) {
		// try {
		// out.writeObject(this.value);
		// out.writeUTF(column.getTable().getSchemaName());
		// out.writeUTF(column.getTable().getName());
		// out.writeUTF(column.getColumnName());
		// } catch (IOException e) {
		//
		// e.printStackTrace();
		// }
		//
		// }

		// @Override
		// public void readObject(ObjectInputStream in) {
		// try {
		// this.value = (Date) in.readObject();
		// this.column = new Column(new Table(in.readUTF(), in.readUTF()),
		// in.readUTF());
		// } catch (IOException | ClassNotFoundException e) {
		// e.printStackTrace();
		// }
		//
		// }

	}

}
