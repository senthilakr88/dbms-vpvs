package edu.buffalo.cse562.structure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.structure.Datum.Row;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class indexrowlistserializer implements Serializer<List<Row>> {

	ArrayList<Column> schema;
	ArrayList<String> type;
	String rowSeparator;

	public indexrowlistserializer(ArrayList<Column> schema,
			ArrayList<String> type) {
		this.schema = schema;
		this.type = type;
		this.rowSeparator = "|";
	}

	@Override
	public List<Row> deserialize(SerializerInput in) throws IOException,
			ClassNotFoundException {
		List<String> rowStr = (List<String>) in.readObject();
		List<Row> rows = new ArrayList<Row>();
		for (int j = 0; j < rowStr.size(); j++) {
			String[] rowTemp = rowStr.get(j).split("\\"+rowSeparator);
			Datum[] row = new Datum[schema.size()];
			for (int i = 0; i < rowTemp.length; i++) {
				Column datumCol = schema.get(i);
				String typeStr = type.get(i).toLowerCase()
						.replaceAll("[^a-z]", "").trim();
				switch (typeStr) {
				case "int":
					row[i] = new Datum.dLong(rowTemp[i], datumCol);
					break;
				case "decimal":
					row[i] = new Datum.dDecimal(rowTemp[i], datumCol, 2);
					break;
				case "string":
				case "varchar":
				case "char":
					row[i] = new Datum.dString(rowTemp[i], datumCol);
					break;
				case "date":
					row[i] = new Datum.dDate(rowTemp[i], datumCol);
					break;
				default:
					throw new IOException(
							"unhandled Type in de serializable :: "
									+ schema.get(i) + " :: " + type.get(i));
				}

			}
			rows.add(new Row(row));
		}
		return rows;
	}

	@Override
	public void serialize(SerializerOutput out, List<Row> rows)
			throws IOException {
		List<String> rowStr = new ArrayList<String>();
		for (int j = 0; j < rows.size(); j++) {
			Row row = rows.get(j);
			String rowTemp = "";
			for (int i = 0; i < schema.size(); i++) {
				String typeStr = type.get(i).toLowerCase()
						.replaceAll("[^a-z]", "").trim();
				switch (typeStr) {
				case "int":
					rowTemp += (((Datum.dLong) row.data[i]).getStringValue())
							+ rowSeparator;
					break;
				case "decimal":
					rowTemp += (((Datum.dDecimal) row.data[i]).getStringValue())
							+ rowSeparator;
					break;
				case "string":
				case "varchar":
				case "char":
					rowTemp += (((Datum.dString) row.data[i]).getValue())
							+ rowSeparator;
					break;
				case "date":
					rowTemp += (((Datum.dDate) row.data[i]).getStringValue())
							+ rowSeparator;
					break;
				default:
					throw new IOException(
							"unhandled Type in out Serializable:: "
									+ schema.get(i) + " :: " + type.get(i));
				}
			}
//			System.out.println(rowTemp);
			rowStr.add(rowTemp);
		}
		out.writeObject(rowStr);
	}

}
