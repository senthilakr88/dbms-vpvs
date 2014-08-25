package edu.buffalo.cse562.physicalPlan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import edu.buffalo.cse562.structure.Datum;
import edu.buffalo.cse562.structure.indexlonglistserializable;
import edu.buffalo.cse562.structure.indexlongserializer;
import edu.buffalo.cse562.structure.indexrowserializer;
import edu.buffalo.cse562.structure.Datum.Row;
import edu.buffalo.cse562.structure.Datum.dDate;
import edu.buffalo.cse562.structure.Datum.dDecimal;
import edu.buffalo.cse562.structure.Datum.dLong;
import edu.buffalo.cse562.structure.Datum.dString;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class IndexNLJoinOperator implements Operator {

	Operator left;
	Datum[] leftTuple;
	Boolean firstEntry;
	String indexName;
	String indexDir;
	String rightTbl;
	String alias;
	Column leftCol;
	Column rightCol;
	List<Long> pkValues;
	Iterator<Long> pkValuesIte;
	RecordManager recMan;
	PrimaryStoreMap<Long, Row> pkStoreMap;
	PrimaryTreeMap<Row, Long> pkTreeMap;
	PrimaryTreeMap<Row, List<Long>> secTreeMap;
	boolean isTupleMapPresent;
	int leftColIndex;
	ArrayList<Column> cols;
	ArrayList<String> type;

	public IndexNLJoinOperator(Operator left, String indexName,
			String indexDir, String rightTbl, String alias, Column leftCol, Column rightCol,
			ArrayList<Column> cols, ArrayList<String> type, RecordManager recMan) {
		this.left = left;
		this.firstEntry = true;
		this.indexName = indexName;
		this.indexDir = indexDir;
		this.rightTbl = rightTbl;
		this.leftCol = leftCol;
		this.rightCol = rightCol;
		this.alias = alias;
		this.isTupleMapPresent = true;
//		System.out.println("Cols in constructor :: "+ cols);
		
		this.cols = cols;
//		if(!rightTbl.equals(alias)) {
//			this.cols = getColList(cols, alias);
//		} else {
//			this.cols = cols;
//		}
		this.type = type;
		try {
			if(recMan == null) {
				this.recMan = RecordManagerFactory.createRecordManager(indexDir
					+ File.separator + rightTbl);
			} else {
				this.recMan = recMan;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		System.out.println("-------------------------------------");
//		System.out.println("rightTbl in NLJoin :: " + rightTbl);
//		System.out.println("indexName in NLJoin :: " + indexName);
//		System.out.println("leftCol in NLJoin :: " + leftCol);
//		System.out.println("rightCol  in NLJoin :: " + rightCol);
//		System.out.println("alias  in NLJoin :: " + alias);
//		System.out.println("cols  in NLJoin :: " + cols);
//		System.out.println("-------------------------------------");
	}

	@Override
	public void resetStream() {

	}

	public Datum[] readOneTuple() {
		Datum[] lt = null, rt = null;
		Datum[] t = null;

		do {
			if (firstEntry) {
				lt = left.readOneTuple();
				if(lt == null) {
					return null;
				}
				
//				System.out.println(" cols for first tuple :: " + cols);
				indexrowserializer datumIndexSer = new indexrowserializer(
						cols, type);
				pkStoreMap = recMan.storeMap(rightTbl + "map",
						datumIndexSer);
//					System.out.println("Size of map :: " + pkStoreMap.size());
				if (!indexName.equalsIgnoreCase(rightTbl)) {
					List<String> tempIdxCols = new ArrayList<String>();
					ArrayList<Column> skColList;
					tempIdxCols.add(rightCol.getColumnName().toLowerCase());
//						System.out.println(TupleStruct.getColString(cols));
//						System.out.println(tempIdxCols);
					List<Integer> secColIndex = TupleStruct.getColPositions(
							TupleStruct.getColString(cols), tempIdxCols);
//						System.out.println("secColIndex :: " + secColIndex);
					
					
//					if(!rightTbl.equals(alias)) {
//						skColList = TupleStruct.getShrColList(cols, secColIndex, alias);
//					} else {
//						skColList = 
//					}
//					System.out.println("Sk ColList :: " + skColList);
					indexrowserializer secIndexSer = new indexrowserializer(
							TupleStruct.getShrList(cols, secColIndex),
							TupleStruct.getShrList(type, secColIndex));
					secTreeMap = recMan.treeMap(indexName,
							new indexlonglistserializable(), secIndexSer);
				} else {
					List<String> tempIdxCols = new ArrayList<String>();
					ArrayList<Column> pkColList;
					tempIdxCols.add(rightCol.getColumnName());
					List<Integer> pkColIndex = TupleStruct.getColPositions(
							TupleStruct.getColString(cols), tempIdxCols);
//						System.out.println("pkColIndex :: " + pkColIndex);
//					if(!rightTbl.equals(alias)) {
//						pkColList = TupleStruct.getShrColList(cols, pkColIndex, alias);
//					} else {
//						pkColList = ;
//					}
//					System.out.println("Pk ColList :: " + pkColList);
					indexrowserializer pkIndexSer = new indexrowserializer(
							TupleStruct.getShrList(cols, pkColIndex),
							TupleStruct.getShrList(type, pkColIndex));
					pkTreeMap = recMan.treeMap(rightTbl,
							new indexlongserializer(), pkIndexSer);
				}
				TupleStruct.setTupleTableMap(lt);
				// System.out.println(TupleStruct.getTupleTableMap());
				// printTuple(lt);
//					System.out.println("leftCol :: " + leftCol);
				leftColIndex = TupleStruct.getColIndex(lt, leftCol);
				// System.out.println(leftColIndex);
//				System.out.println("setting Left tuple initially");
				setLeftTuple(lt);
//				System.out.println("setting right tuple initially");
				setRightTuple(lt);
				// System.out.println("entry");
				firstEntry = false;

			}
//			System.out.print("Datum :: ");
			
			lt = getLeftTuple();
//			System.out.println("Getting left from previous stored " + alias);
//			printColumnTuple(lt);
//			System.out.println("cols for second tuple :: "+cols);
			if (lt != null) {
//				System.out.println("Getting right from previous stored 1 " + alias);
				rt = getRightTuple();
				if (rt == null) {
//					System.out.println("Right empty" + alias);
//					System.out.println("Getting Left from oper " + alias);
					lt = left.readOneTuple();
//					printColumnTuple(lt);
					if (lt == null) {
//						 System.out.println("Left empty :: " + alias);
//						try {
//							recMan.close();
//						} catch (IOException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
						clearObjects();
						return null;
					}
//					System.out.println("refilling left "+ alias);
					setLeftTuple(lt);
//					System.out.println("refilling right "+ alias);
					setRightTuple(lt);
//					System.out.println("Getting right from previous stored 2 "+ alias);
					rt = getRightTuple();
					if (rt == null) {
						continue;
					}
				}
//				System.out.println("combining left and right " + alias);
				t = combine(lt, rt);

				if (t == null) {
					return null;
				}
			} else {
				return null;
			}
		} while (t == null);
		// System.out.println("Came here 2");
		// System.out.println("///////////return tuple length"+t.length);
//		System.out.println("returning with a tuple " + alias);
//		printColumnTuple(t);
		return t;

	}

	private void clearObjects() {
		leftTuple = null;
		indexName = null;
		indexDir = null;
		rightTbl = null;
		leftCol = null;
		pkValues = null;
		pkValuesIte = null;
		recMan = null;
		pkStoreMap = null;
		pkTreeMap = null;
		secTreeMap = null;
		rightCol = null;
		cols = null;
		type = null;
	}

	private Datum[] getRightTuple() {
		Datum[] temp = null;
		if (pkValues == null || pkValues.size() < 1) {
			pkValues = null;
			pkValuesIte = null;
			return null;
		} else {
			while (pkValuesIte.hasNext()) {
				Long pkRt = pkValuesIte.next();
				Row row = pkStoreMap.get(pkRt);
//				System.out.println(cols);
				temp = row.getDatum();
//				printTuple(temp);
//				printColumnTuple(leftTuple);
//				printColumnTuple(temp);
				if(!rightTbl.equals(alias)) {
					temp = setAlias(temp);
				}
//				System.out.println("alias :: "+alias);
//				printColumnTuple(leftTuple);
//				printColumnTuple(temp);
				return temp;
			}
			if (!pkValuesIte.hasNext()) {
				pkValues = null;
				pkValuesIte = null;
				return null;
			}
		}
		return temp;
	}

	private Datum[] setAlias(Datum[] row) {
		Datum[] temp = null;
		if (row != null && row.length != 0) {
			temp = new Datum[row.length];
			for(int i =0;i<row.length;i++) {
//				System.out.println(row.length + " :: "+row[i] + " :: " + row[i].getColumn().getTable() + " :: " +row[i].getColumn() + " :: " + alias);
				Datum tempDatum = null;
				Datum ob = row[i];
				if (ob instanceof dLong) {
					tempDatum = new dLong((dLong)ob);
					tempDatum.setColumn(ob.getColumn());
					tempDatum.setAliasName(alias);
				} else if (ob instanceof dDecimal) {
					tempDatum = new dDecimal((dDecimal)ob);
					tempDatum.setColumn(ob.getColumn());
					tempDatum.setAliasName(alias);
				} else if (ob instanceof dString) {
					tempDatum = new dString((dString)ob);
					tempDatum.setColumn(ob.getColumn());
					tempDatum.setAliasName(alias);
				} else if (ob instanceof dDate) {
					tempDatum = new dDate((dDate)ob);
					tempDatum.setColumn(ob.getColumn());
					tempDatum.setAliasName(alias);
				} else {
					try {
						throw new Exception("Projection Not aware of this data type " + ob.getStringValue() + ob.getColumn());
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
				
				temp[i] = tempDatum;
//				System.out.println(row.length + " :: "+row[i] + " :: " + row[i].getColumn().getTable() + " :: " +row[i].getColumn() + " :: " + alias);
//				System.out.println(row.length + " :: "+temp[i] + " :: " + temp[i].getColumn().getTable() + " :: " +temp[i].getColumn() + " :: " + alias);
			}
//			System.out.println("____________________________________________________________");
		}
		return temp;
	}

	private void setRightTuple(Datum[] lt) {
		if (lt == null) {
			pkValues = null;
			return;
		}
		if (indexName.equalsIgnoreCase(rightTbl)) {
			Long key;
			pkValues = new ArrayList<Long>();
			Datum[] datum = new Datum[1];
			datum[0] = lt[leftColIndex];
			key = pkTreeMap.get(new Row(datum));
			if (key == null) {
				pkValues = null;
			} else {
				pkValues.add(key);
				pkValuesIte = pkValues.iterator();
			}
		} else {
			Datum[] datum = new Datum[1];
			datum[0] = lt[leftColIndex];
//			 printTuple(datum);
//			System.out.println("Index join sec treee size :: "+ secTreeMap.size());
//			System.out.println("sectree key set :: " + secTreeMap.keySet());
			pkValues = secTreeMap.get(new Row(datum));
//			System.out.println("pkValues :: "+pkValues);
			if(pkValues !=null)
			pkValuesIte = pkValues.iterator();
		}
	}

	private Datum[] combine(Datum[] lt, Datum[] rt) {
		int i = 0, j = 0;
		Datum[] temp = new Datum[lt.length + rt.length];
//		printColumnTuple(lt);
//		printColumnTuple(rt);
//		printTuple(lt);
//		printTuple(rt);
		for (i = 0; i < lt.length; i++) {
			temp[i] = lt[i];
			// System.out.println(lt[i].toComString());
		}
		for (j = 0; j < rt.length; j++, i++) {
			temp[i] = rt[j];
			// System.out.println(rt[j].toComString());
		}
		return temp;
	}

	public Datum[] getLeftTuple() {
		return leftTuple;
	}

	public void setLeftTuple(Datum[] lt) {
		this.leftTuple = lt;
	}

	@Override
	public void resetTupleMapping() {
		this.isTupleMapPresent = true;
	}
	
//	public ArrayList<Column> getColList(ArrayList<Column> masterList, String alias) {
//		ArrayList<Column> copyList = new ArrayList<Column>();
//		for(int i=0;i<masterList.size();i++) {
//			Column colTemp = new Column(masterList.get(i).getTable(), masterList.get(i).getColumnName());
//				if(alias != null)
//					colTemp.setTable(new Table(colTemp.getTable().getSchemaName(), alias));
//				copyList.add(colTemp);
//		}
//		return copyList;
//	}

	private void printTuple(Datum[] row) {
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
			System.out.println();
		}
	}
	

	private void printColumnTuple(Datum[] row) {
		Boolean first = true;
		if (row != null && row.length != 0) {
			for (Datum col : row) {
				if (!first)
					System.out.print("|" +col.getColumn().getTable().getAlias());
				else {
					System.out.print(col.getColumn().getTable().getAlias());
					first = false;
				}
			}
			System.out.println();
		}
	}

}
