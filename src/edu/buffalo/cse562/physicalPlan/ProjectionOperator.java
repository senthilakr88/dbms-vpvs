package edu.buffalo.cse562.physicalPlan;

//import net.sf.jsqlparser.expression.Expression;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;



import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
//import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.logger.logManager;
import edu.buffalo.cse562.physicalPlan.Operator;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import edu.buffalo.cse562.sql.expression.evaluator.SelectItemTools;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.Datum.*;

public class ProjectionOperator implements Operator {

	Operator input;

	String temp = null;
	List<SelectExpressionItem> selectcolumns;
	boolean isTupleMapPresent;

	public ProjectionOperator(Operator input,
			List<SelectExpressionItem> selectcolumns) {
		this.selectcolumns = selectcolumns;
		this.input = input;
		this.isTupleMapPresent = true;
	}


	public void resetStream() {
		input.resetStream();

	}

	public Datum[] readOneTuple() {
		Datum[] t = null;
		Datum[] listDatum;
		ArrayList<Datum> arrayListDatum = new ArrayList<Datum> ();
		
		t = input.readOneTuple();
		if (t != null) {
			int i=0;
			//System.out.println("Entering TupleMapvalue ::"+ isTupleMapPresent);
			if(isTupleMapPresent) {
				TupleStruct.setTupleTableMap(t);
				if(!TupleStruct.isNestedCondition())
					isTupleMapPresent = false;
//				System.out.println("_____"+TupleStruct.getTupleTableMap());
			}
			Iterator<SelectExpressionItem> iter=selectcolumns.iterator();
			//to compute col type index
			int k = 0;
			listDatum = new Datum[selectcolumns.size()];
			while(iter.hasNext()){
				SelectItem newItem = iter.next();
				SelectItemTools st = new SelectItemTools();
				newItem.accept(st);
				String stType = st.getItemType();
//				System.out.println(newItem.getClass().getName());
//				System.out.println(i);
				if(stType.equalsIgnoreCase("AllColumns")){
					//					listDatum = t;
				} else if (stType.equalsIgnoreCase("AllTableColumns")){
					String requestedTableName = ((AllTableColumns) newItem).getTable().toString();
//					System.out.println("Received request for---"+((AllTableColumns) newItem).getTable());
					for (int iterable=0;iterable<t.length;iterable++){
//						System.out.println("In the tuple----"+t[iterable].getColumn().getTable().toString());
						if(t[iterable].getColumn().getTable().getAlias()!=null){
							if(t[iterable].getColumn().getTable().getAlias().toString().equalsIgnoreCase(requestedTableName)){
//								System.out.println("Matched alias name");
								arrayListDatum.add(t[iterable]);
							}
						} else {
							if(t[iterable].getColumn().getTable().toString().equalsIgnoreCase(requestedTableName)){
//								System.out.println("Matched table name");
								arrayListDatum.add(t[iterable]);
							}
						}
					}
					//					listDatum[i]=new Datum.dString("SsssS", new Column(null,"sdfdfs"));
				} else if (stType.equalsIgnoreCase("SelectExpressionItem")){
//					System.out.println(((SelectExpressionItem) newItem).getExpression().toString());
					Expression e = ((SelectExpressionItem) newItem).getExpression();
//					System.out.println("Entering Projection expression :: "+e.toString());
//					TupleStruct.setTupleTableMap(t);
//					System.out.println(TupleStruct.getTupleTableMap());
					CalcTools calc = new CalcTools(t); 
					e.accept(calc);
					//                System.out.println("PRinting column name--->"+calc.getColumn().getColumnName());
					Column newCol = null;
					//                Table result = new Table("", "ResultTable");
//					System.out.println("In Projection Statement " + ((SelectExpressionItem) newItem).getExpression().toString());
					if (((SelectExpressionItem) newItem).getAlias()!=null){
//						System.out.println("In Projection Statement " + ((SelectExpressionItem) newItem).getAlias());
						newCol = new Column(null, ((SelectExpressionItem) newItem).getAlias());
					}
					else {
//						System.out.println("Entering to get column in projection :: ");
						newCol = calc.getColumn();
						//                    newCol = new Column(result, newItem.toString());
					}
//					System.out.println("In projection operator:: "+newCol.getWholeColumnName());
					
   				    Datum ob = calc.getResult();
					Boolean isColumn = calc.isColumn();
					Datum tempDatum = null;
					if (ob instanceof dLong) {
						tempDatum = new dLong((dLong)ob);
						tempDatum.setColumn(newCol);
					} else if (ob instanceof dDecimal) {
						tempDatum = new dDecimal((dDecimal)ob);
						tempDatum.setColumn(newCol);
						if(isColumn!=null&&isColumn==true){
							((dDecimal)tempDatum).setPrecision(2);
						} else {
							((dDecimal)tempDatum).setPrecision(4);
						}

					} else if (ob instanceof dString) {
						tempDatum = new dString((dString)ob);
						tempDatum.setColumn(newCol);
					} else if (ob instanceof dDate) {
						tempDatum = new dDate((dDate)ob);
						tempDatum.setColumn(newCol);
					} else {
						try {
							throw new Exception("Projection Not aware of this data type " + ob.getStringValue() + ob.getColumn());
						} catch (Exception e1) {
							e1.printStackTrace();
						}
					}
					listDatum[k] = tempDatum;
					k++;
					i++;

				}
			}
			//			for(int index=0;index<listDatum.length;index++){
			//				System.out.println(listDatum[index].toString());
			//			}
			//			System.out.println(listDatum.toString());


		} else {
			return null;
		}
//		listDatum = arrayListDatum.toArray(listDatum);
//		System.out.println(arrayListDatum.toString());
//		System.out.println("Reporting from projection");
//		printTuple(listDatum);
		return listDatum;

	}


	@Override
	public void resetTupleMapping() {
		isTupleMapPresent = true;
		
	}
	
	public void printTuple(Datum[] row) {
		Boolean first = true;
		if (row != null && row.length != 0) {
			for (Datum col : row) {
				if (!first)
					System.out.print("|" + col);
				else {
					System.out.print(col);
					first = false;
				}
			}
			// System.out.println();
		}
	}
}

