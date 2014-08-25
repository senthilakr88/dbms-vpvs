package edu.buffalo.cse562.structure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.structure.Datum.Row;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class indexrowserializer implements Serializer<Row> {

	ArrayList<Column> schema;
	ArrayList<String> type;
	
	public indexrowserializer(ArrayList<Column> schema, ArrayList<String> type) {
		this.schema = schema;
		this.type = type;
	}
	
	@Override
	public Row deserialize(SerializerInput in) throws IOException,
			ClassNotFoundException {
		
		Datum[] row = new Datum[schema.size()];
		for(int i=0;i<schema.size();i++) {
			Column datumCol = schema.get(i);
//			if(i > 0)
//				System.out.println("schema "+i+" :: "+datumCol.getTable());
			String typeStr = type.get(i).toLowerCase().replaceAll("[^a-z]","").trim();
			switch(typeStr) {
			case "int":
				row[i] = new Datum.dLong(in.readLong(),datumCol);
				break;
			case "decimal":
				row[i] = new Datum.dDecimal(in.readDouble(),datumCol,2);
				break;
			case "string":
			case "varchar":
			case "char":
				row[i] = new Datum.dString(in.readUTF(),datumCol);
				break;
			case "date":
				row[i] = new Datum.dDate(in.readUTF(), datumCol);
				break;
			default:
				throw new IOException("unhandled Type in de serializable :: " + schema.get(i) + " :: "+ type.get(i));
			}
		}
//		System.out.println("row :: "+new Row(row));
		return new Row(row);
	}

	@Override
	public void serialize(SerializerOutput out, Row row) throws IOException {
//		System.out.println(row);
//		System.out.println(schema);
//		if(schema.size() > 1)
//		System.out.println("schema 2 :: "+schema);
		for(int i=0;i<schema.size();i++) {
			String typeStr = type.get(i).toLowerCase().replaceAll("[^a-z]","").trim();
			switch(typeStr) {
			case "int":
				out.writeLong(((Datum.dLong)row.data[i]).getValue());
				break;
			case "decimal":
				out.writeDouble(((Datum.dDecimal)row.data[i]).getValue());
				break;
			case "string":
			case "varchar":
			case "char":
				out.writeUTF(((Datum.dString)row.data[i]).getValue());
				break;
			case "date":
				out.writeUTF(((Datum.dDate)row.data[i]).getStringValue());
				break;
			default:
				throw new IOException("unhandled Type in out Serializable:: " + schema.get(i) + " :: "+ type.get(i));
			}
		}
		
	}

}
