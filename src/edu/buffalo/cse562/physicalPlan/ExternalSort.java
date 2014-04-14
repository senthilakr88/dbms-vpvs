package edu.buffalo.cse562.physicalPlan;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import edu.buffalo.cse562.sql.expression.evaluator.ColumnFetcher;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class ExternalSort implements Operator {

	Operator oper;
	String swapDir;
	// BufferedReader reader, reader1;
	Map<Integer, ArrayList<Datum[]>> buffer;
	Integer bufferMaxSize;
	// HashMap<Integer, ArrayList<Datum[]>> hmap;
	ArrayList<Datum[]> result = null;
	Integer resultIndex;
	ArrayList<BufferedReader> buffread = null;
	List<OrderByElement> elements;
	List<Integer> index;
	String masterFile;
	BufferedReader masterBuffer;
	String tableName;
	List<Boolean> asc;
	Boolean first;
	Integer kWay;
	Integer capacity;
	Map<String, ArrayList<Column>> tableMap;
	Map<String, ArrayList<String>> tableColTypeMap;
	Boolean preSet;
	Integer[] colType;
	Column[] colList;
	int colCount;

	// Constructor of ExternalSort
	public ExternalSort(Operator oper, String tableName, List elements,
			String swapDir, Integer countTable) {
//		printMemory("Constructor Starts");
		this.oper = oper;
		this.elements = elements;
		this.swapDir = swapDir;
//		if(countTable >= 2) {
//			this.bufferMaxSize = 40000;
//		} else {
		this.bufferMaxSize = 100000;
//		}
		this.kWay = 7;
		this.capacity = 1000;

		this.first = true;
		this.preSet = true;
		this.tableName = tableName;
		this.resultIndex = 0;

		// System.out.println(this.swapDir);
		if (!(new File(this.swapDir).exists())) {
			this.swapDir = new File("").getAbsolutePath() + File.separator
					+ this.swapDir;
		}
		this.swapDir = this.swapDir + File.separator + tableName
				+ (countTable == null ? 0 : countTable) + "_";
		this.masterFile = this.swapDir + tableName + ".ser";

		// hmap = new HashMap<Integer, ArrayList<Datum[]>>();
		asc = new ArrayList<Boolean>();
		buffer = new HashMap<Integer, ArrayList<Datum[]>>();
		buffread = new ArrayList<BufferedReader>();
		sortfile();
		clearbuffer();
//		printMemory("Constructor Ends");
	}

	private void clearbuffer() {
		buffer = null;
		bufferMaxSize = null;
		result = null;
		resultIndex = null;
		buffread = null;
		elements = null;
		index = null;
		tableName = null;
		asc = null;
		first = null;
		kWay = null;
		capacity = null;
		tableMap = null;
		tableColTypeMap = null;
		preSet = null;
	}

	public boolean readFile(BufferedReader br, int i, int capacity) {

		try {
//			printMemory("Readfile Starts");
			// System.out.println("br.available :: "+br.available());
			ArrayList<Datum[]> datum = new ArrayList<Datum[]>(capacity);
			Datum[] tempDatum = null;
			int k = 0;
			String fileEntry = br.readLine();
//			System.out.println(tableName + " outside k :: "+ k + " capacity :: " + capacity + " :: "+ " fileEntry ::"+fileEntry);
			if (fileEntry != null) {
				while (k < capacity-1) {
					tempDatum = convertStrToDatum(fileEntry);
					datum.add(tempDatum);
					k++;
					fileEntry = br.readLine();
//					System.out.println(tableName + " inside k :: "+ k + " capacity :: " + capacity + " :: "+ " fileEntry ::"+fileEntry);
					if (fileEntry == null) {
						buffer.put(i, datum);
						return true;
					}
				}
				if(fileEntry!=null) {
					tempDatum = convertStrToDatum(fileEntry);
					datum.add(tempDatum);
				}
				buffer.put(i, datum);
//				printMemory("Readfile End");
				return true;
			} else {
//				printMemory("Readfile End");
				br.close();
				return false;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private Datum[] convertStrToDatum(String datumStr) {
//		printMemory("convertStrToDatum starts");
		String[] datumfield;
		Datum[] tempDatum;

//		System.out.println("-------------------------------------------------------------------");
//		System.out.println(tableName + " ::  datumStr :: " + datumStr + " colList :: "+colList+" colType :: "+colType);		
//		System.out.println(tableName + " ::  colList :: " + Arrays.asList(colList));
//		System.out.println(tableName + " ::  colType :: " + Arrays.asList(colType));

		if (datumStr != null) {
			tempDatum = new Datum[colCount];
			datumfield = datumStr.split("\\|");
//			System.out.println(" datumfield :: " + Arrays.asList(datumfield));
			for (int i = 0; i < datumfield.length; i++) {
//				System.out.println("datumfield :: "+ datumfield[i] + " colType[i] :: "+ colType[i]);
				if (colType[i] == 0) {
					tempDatum[i] = new Datum.dLong(datumfield[i], colList[i]);
				} else if (colType[i] == 1) {
//					System.out.println("Decimal Field Length :: " +datumfield[i].length());
//					System.out.println("Decimal Dot position :: " + datumfield[i].indexOf("."));
					tempDatum[i] = new Datum.dDecimal(datumfield[i],
							colList[i], datumfield[i].length() - datumfield[i].indexOf(".")-1);
				} else if (colType[i] == 2) {
					tempDatum[i] = new Datum.dString(datumfield[i], colList[i]);
				} else if (colType[i] == 3) {
					tempDatum[i] = new Datum.dDate(datumfield[i], colList[i]);
				}
			}
//			System.out.println("-------------------------------------------------------------------");
//			printMemory("convertStrToDatum End");
			return tempDatum;
		} else {
//			printMemory("convertStrToDatum End");
			return null;
		}


	}

	public boolean createNewFile(String fileName) {
		try {
//			printMemory("fileName start");
			File file = new File(fileName);
			if (!file.exists()) {
//				printMemory("fileName end");
				return file.createNewFile();
			} else {
				file.delete();
//				printMemory("fileName end");
				return file.createNewFile();

			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	// Used to create a writer for an object stream
	public void writedata(BufferedWriter out, int i) {
		try {
			// System.out.println(out.toString());
//			printMemory("writedata start");
			if (i < 0) {
				convertDatumToStr(out, result);
				out.flush();
			} else {
				convertDatumToStr(out, buffer.get(i));
				out.flush();
			}
//			printMemory("writedata ends");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String convertDatumToStr(BufferedWriter out, ArrayList<Datum[]> datumList) {
//		printMemory("convertDatumtoStr start");
		String datumString = "";
		try {
		if (datumList != null) {
			Iterator<Datum[]> ite = datumList.iterator();
			while (ite.hasNext()) {

					out.write(convertDatum(ite.next()));

			}
		  } 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		System.out.println("datumString :: "+datumString);
//		printMemory("convertDatumtoStr ends");
		return datumString;
	}

	public String convertDatum(Datum[] row) {
//		printMemory("convertdatum start");
		Boolean first = true;
		StringBuilder sb = new StringBuilder();
		if (row != null && row.length != 0) {
			for (Datum col : row) {
				if (!first)
					sb.append("|" + col);
				else { 
					sb.append(col);
					first = false;
				}
			}
			sb.append("\n");
		}
//		System.out.println("sb :: "+sb);
//		printMemory("convertdatum end");
		return sb.toString();
	}

	// used to create a writer object stream
	public BufferedWriter writeOS(String fileName) {
//		printMemory("writeOS start");
		BufferedWriter out = null;
		try {
			// out = new ObjectOutputStream(new FileOutputStream((new
			// RandomAccessFile(fileName, "rw")).getFD()));
			out = new BufferedWriter(new FileWriter(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
//		printMemory("writeOS end");
		return out;
	}

	// used to create an reader object stream
	public BufferedReader readOS(String fileName) {
//		printMemory("readOS start");
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
//		printMemory("readOS end");
		return in;
	}

	boolean readfile(int i, int depth) {
//		System.out.println("Entering readfile ");
//		printMemory("readfile start");
		Datum[] oneTupleFromDat = null;
		ArrayList<Datum[]> tempDatumList = new ArrayList<Datum[]>(bufferMaxSize);
		int count = 0;
		oneTupleFromDat = oper.readOneTuple();
//		System.out.println(tableName + " :: Entering :: " + preSet + " :: "+ oneTupleFromDat);
		if (oneTupleFromDat != null && preSet) {

//			printTuple(oneTupleFromDat);
			TupleStruct.setTupleTableColMap(oneTupleFromDat);
			colList = TupleStruct.getTupleTableColMap();
			colType = TupleStruct.getTupleTableColTypeMap();
			colCount = colList.length;
//			System.out.println("----------------------------------------------");
//			System.out.println(tableName + " ::  set col :: "+ Arrays.asList(colList));
//			System.out.println(tableName + " ::  set type :: "+Arrays.asList(colType));
//			System.out.println("----------------------------------------------");
			preSet = false;
		}
		// buffer = new ArrayList<Datum[]>();
		while (oneTupleFromDat != null) {
//			System.out.println("Reading tuples from main stream ");
//			printTuple(oneTupleFromDat);
			tempDatumList.add(oneTupleFromDat);
			count++;
			if (count < bufferMaxSize) {
				oneTupleFromDat = oper.readOneTuple();



			} else {
				break;
			}
		}
		buffer.put(i, tempDatumList);
		if (buffer.get(i).size() > 0) {
//			System.out.println("returning readfile with true");
//			printMemory("readfile end");
			return true;
		} else {
//			System.out.println("returning readfile with false");
			buffer.remove(i);
//			printMemory("readfile end");
			return false;
		}

	}

	// This function return sort the buffer in first pass
	int readpage() {
//		System.out.println("entering readpage");
//		printMemory("readpage start");
		int i = 0;
		// int runs = 0;
		// String s1 = null;
		String s = null, index = null;
		// This function gets the first buffer

		try {
			while (readfile(i, 1)) {
//				 System.out.println("Entering to sort Data :: ");
//				 printTuple(buffer);
				sortdata(i);
//				oper.resetTupleMapping();
				// System.out.println(swapDir);
				s = swapDir + "buffer[";
				index = Integer.toString(1) + Integer.toString(i);
				s = s + index + "].ser";
				BufferedWriter out = null;
				if (createNewFile(s)) {
					out = writeOS(s);
				} else {
					System.out.println("unable to create a file @ " + s);
				}
				writedata(out, i);
				out.close();
				buffer = null;
				buffer = new HashMap<Integer, ArrayList<Datum[]>>();
//				writedata(out, i);
				i++;
			}
//			 System.out.println("printing tuples from readpage");
			// printTuple(buffer);
//			printMemory("readpage end");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return i;
	}

	public void sortfile() {
		// Initial sort, completed phase 1
//		System.out.println("entering sort file");
//		printMemory("sort file start");
		int runs = readpage();
//		System.out.println("Read page completed :: "+runs);
		if (runs == 1) {
			// System.out.println(swapDir);
			String s = swapDir + "buffer[10].ser";
			File buffername = new File(s);

			File file2 = new File(masterFile);
			if (!buffername.renameTo(file2)) {
				System.out.println("File renaming failed");
			}
			return;
		}
		if (runs == 0) {
			BufferedWriter out = null;
			if (createNewFile(masterFile)) {
				out = writeOS(masterFile);
			} else {
				System.out.println("unable to create a file @ " + masterFile);
			}
			try {
				// out.write(null);
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		int count = 0;
		String s = null;

		int i = 0, k = 0, runcurrent = 0, depth = 1, filenumber = 0;
		boolean check = true;
		while (i < runs && check) {
			// if(depth == 2)
			// kWay = 2;
			if (runs - i >= kWay) {
				k = kWay;
//				if (runs - i - k < 3) {
//					k = runs - i;
//				}
			} else {
				k = runs - i;
			}

			count = k;
			while (count > 0) {

				s = swapDir + "buffer[";
				String index = Integer.toString(depth) + Integer.toString(i);
				s = s + index + "].ser";

				buffread.add(readOS(s));

				readFile(buffread.get(i), i, capacity);

				count--;
				i++;
			}

			runcurrent++;
//			 System.out.println("depth :: " + depth + " k :: " + k);
			if (runcurrent == 1 && i == runs) {
				secondsort(i - k, k, filenumber, 0);
				buffer = new HashMap<Integer, ArrayList<Datum[]>>();
				//buffread = new ArrayList<BufferedReader>();
			} else {
				secondsort(i - k, k, filenumber, depth);
				buffer = new HashMap<Integer, ArrayList<Datum[]>>();
				//buffread = new ArrayList<BufferedReader>();
			}

			filenumber++;

			if (i == runs) {
				if (runcurrent == 1) {
					check = false;
					break;
				}
				runs = runcurrent;
				runcurrent = 0;
				filenumber = 0;
				depth = depth + 1;
				k = 0;
				i = 0;
				buffer = new HashMap<Integer, ArrayList<Datum[]>>();
				buffread = new ArrayList<BufferedReader>();
			}
		}
//		printMemory("sortpage end");
//		System.out.println("moving out of sort file");
	}

	void secondsort(int start, int count, int filenumber, int depth) {
//		printMemory("secondsort start");
		BufferedWriter out = null;
		int counter = 1;
		int numberofemptylists = 0;

		int k = start;
		Datum[] lowest = null;
//		ArrayList<Datum[]> list1 = null;
		Datum[] element;
		int removeindex = -1;
		boolean check = true;
		ArrayList<Integer> indexTraversal = new ArrayList<Integer>();
		for (k = start; k < (start + count); k++)
			indexTraversal.add(k);

		Iterator<Integer> itIndexTrav = indexTraversal.iterator();

		k = 0;
		try {
			result = new ArrayList<Datum[]>();
			while (result.size() < capacity && check) {
				while (itIndexTrav.hasNext()) {

					k = itIndexTrav.next();
					// System.out.println("start :: "+start+" :: "+count);
					// System.out.println("k :: " + k);
//					System.out.println("----------------------------------------------");
//					printTuple(buffer);
//					System.out.println(buffer.containsKey(k));
//					System.out.println(buffer.values() !=null);
//					System.out.println(buffer.values().size() > 0);
//					
//					System.out.println("----------------------------------------------");
					// System.out.print("list1 :: ");
					// printTuple(list1);
					element = (buffer.containsKey(k) && buffer.get(k).size() > 0) ? buffer.get(k).get(0) : null;
//					if (list1 != null && list1.size() > 0) {
					if (element != null) {
//						element = list1.get(0);
						if (first) {
							TupleStruct.setTupleTableMap(element);
							TupleStruct.setTupleTableColMap(element);
							computeIndex(element);
							colList = TupleStruct.getTupleTableColMap();
							colType = TupleStruct.getTupleTableColTypeMap();
							colCount = colList.length;
//							System.out.println("----------------------------------------------");
//							System.out.println(tableName + " ::  first :: "+ first);
//							System.out.println(tableName + " ::  set col :: "+ Arrays.asList(colList));
//							System.out.println(tableName + " ::  set type :: "+Arrays.asList(colType));
//							System.out.println("----------------------------------------------");
							first = false;
						}

						if (lowest == null) {
							lowest = element;
							removeindex = k;

						} else if (compare(element, lowest) <= 0) {
							lowest = element;
							removeindex = k;

						}

						// System.out.println("lowest selected :: ");
						// printTuple(lowest);
						// System.out.println("element compared :: ");
						// printTuple(element);
						// System.out.println(" removeindex :: "+ removeindex);

					} else {

						if (readFile(buffread.get(k), k, capacity)) {
							itIndexTrav = indexTraversal.iterator();
							continue;
						} else {

							itIndexTrav.remove();
							if (buffer.get(k) != null
									&& buffer.get(k).size() >= 0) {
								// System.out.println("removing buffer");
								buffer.remove(k);
							}
							numberofemptylists++;
							if (numberofemptylists == (count - 1)) {
								// System.out.println("writing to list");

								element = null;
								if (counter == 1) {
									String s = computeFile(depth, filenumber);
									//
									if (createNewFile(s)) {
										out = writeOS(s);
									} else {
										System.out
												.println("unable to create a file @ "
														+ s);
									}
									//
									counter = 0;
								}
								if (result.size() > 0) {
									// System.out.print("result last :: ");
									// printTuple(result);
									// System.out.println();
									writedata(out, -2);
								}

								result = null;

								int m = indexTraversal.get(0);
								if (buffer.get(m).size() > 0) {
									// System.out.print("buffer last :: ");
									// printTuple(buffer.get(m));
									// System.out.println();
									writedata(out, m);
								}

								// System.out.println("temp buffer :: ");
								// printTuple(buffer);
								// System.out.println("temp result :: ");
								// printTuple(result);
								// System.out.println("temp buffread :: ");
								// System.out.println(buffread);

								while (readFile(buffread.get(m), m, capacity)) {
									// System.out.print("buffer last fin :: ");
									// printTuple(buffer.get(m));
									// System.out.println();
									writedata(out, m);

								}
								buffer.remove(m);
								// buffread = new
								// ArrayList<ObjectInputStream>();
								// break;

								// }

								check = false;
								// System.out.println("Changing check value :: "
								// + check);
								// list1 = null;
								indexTraversal = new ArrayList<Integer>();
								break;

							}

						}

					}
				}

				if (!check)
					break;

				result.add(lowest);
				// System.out.println();
				// System.out.print("lowest added :: ");
				// printTuple(lowest);
				// System.out.println();
				// System.out.println(indexTraversal);
				if (removeindex != -1) {
					lowest = null;
					if (buffer != null && buffer.get(removeindex).size() > 0)
						buffer.get(removeindex).remove(0);
				} else {

					throw new Exception("Invalid Index in external sort"
							+ removeindex);
				}
				itIndexTrav = indexTraversal.iterator();
				if (result.size() == capacity) {

					if (counter == 1) {
						String s = computeFile(depth, filenumber);
						if (createNewFile(s)) {
							out = writeOS(s);
						} else {
							System.out
									.println("unable to create a file @ " + s);
						}
						counter = 0;
					}
					element = null;
					removeindex = -1;
					// System.out.println("comp buffer :: ");
					// printTuple(buffer);
					// System.out.println("comp result :: ");
					// printTuple(result);
					// System.out.println("comp buffread :: ");
					// System.out.println(buffread);

					writedata(out, -2);
					result = new ArrayList<Datum[]>();
				}
			}
			result = null;
			writedata(out, -2);
			out.close();
//			printMemory("secondsort end");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public int binarySearch(ArrayList<Datum[]> searchList, Datum[] searchDatum) {
		int low = 0, high = 0, mid = 0;
		if (searchList == null || searchList.size() > 0) {
			low = 0;
			high = searchList.size() - 1;
			while (compare(searchList.get(low), searchList.get(high)) <= 0) {
				mid = low + high / 2;
				if (compare(searchList.get(mid), searchDatum) > 0) {
					high = mid - 1;
				} else if (compare(searchList.get(mid), searchDatum) < 0) {
					low = mid + 1;
				} else {
					return mid;
				}
			}
		}
		return low - 1;
	}

	String computeFile(int depth, int i) {
//		printMemory("computeFile start");
		String s = null, index = null;
		if (depth == 0) {
			s = masterFile;
		} else {
			s = swapDir + "buffer[";
			index = Integer.toString(depth + 1) + Integer.toString(i);
			s = s + index + "].ser";
		}
//		printMemory("computeFile end");
		return s;
	}

	void sortdata(int i) {
		Collections.sort(buffer.get(i), new Mysorter(elements));
	}

	public void computeIndex(Datum[] tuple) {
//		printMemory("computeIndex start");
		int ind;
		Column col;
		index = new ArrayList<Integer>();
		Iterator iter = elements.iterator();
		while (iter.hasNext()) {
			OrderByElement ele = (OrderByElement) iter.next();
			Expression exe = ele.getExpression();
			ColumnFetcher cf = new ColumnFetcher();
			exe.accept(cf);
			col = cf.getCol();
			if (col != null) {
				ind = TupleStruct.getColIndex(tuple, col);
				if (ind != -1) {
					index.add(ind);
					asc.add(ele.isAsc());
				} else {
					System.out
							.println("Index not fetched properly :: ExternalSort");
				}

			} else {
				System.out
						.println("Column not fetched properly :: ExternalSort");
			}

		}
//		printMemory("computeIndex end");
	}

	public int compare(Datum[] t1, Datum[] t2) {
//		printMemory("compare start");
		int comparison = -2;
		int k = 0;
		int indexSize = index.size();
		int ind;
		while (k < indexSize) {
			ind = index.get(k);
			comparison = TupleStruct.getCompareValue((Object) t1[ind],
					(Object) t2[ind], asc.get(k));

			if (comparison != 0) {

				return comparison;
			}
			k++;
		}
//		printMemory("compare end");
		return comparison;

	}

	@Override
	public void resetStream() {
		oper.resetStream();
	}

	@Override
	public Datum[] readOneTuple() {
//		printMemory("readonetuple start");
		Datum[] tuple = null;
		String readFile = null;
		try {
//			System.out.println("Reading out tuple " + tableName);
			if(this.masterBuffer == null) {
				this.masterBuffer = readOS(masterFile);
			}
			readFile = masterBuffer.readLine();
			if (readFile == null) {
				masterBuffer.close();
				return null;
			} else {
				return convertStrToDatum(readFile);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println(masterFile + " :: External Sort :: ");
		// printTuple(tuple);
		// System.out.println();
//		printMemory("readonetuple end");
		return tuple;
	}

	@Override
	public void resetTupleMapping() {

	}

	public void printTuple(Map<Integer, ArrayList<Datum[]>> hashTable) {
		Iterator it = hashTable.entrySet().iterator();
		ArrayList<Datum[]> tempList;
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			System.out.print(pairs.getKey() + " = ");
			tempList = (ArrayList<Datum[]>) pairs.getValue();
			Iterator it1 = tempList.iterator();
			while (it1.hasNext()) {
				printTuple((Datum[]) it1.next());
				System.out.print(",");
			}
			System.out.println();
		}
	}

	public void printTuple(Datum[] row) {
		Boolean first = true;
		if (row != null && row.length != 0) {
			// for (Datum col : row) {
			// if (!first)
			// System.out.print("|" + col);
			// else {
			// System.out.print(col);
			// first = false;
			// }
			// }
			// System.out.println();
			System.out.print(row[0].toString());
		}
	}

	public void printTuple(ArrayList<Datum[]> row) {
		if (row != null) {
			Iterator ite = row.iterator();
			while (ite.hasNext()) {
				printTuple((Datum[]) ite.next());
				System.out.print(" , ");
			}
			System.out.println();
		}
	}

	public void printMemory(String name) {

		System.out.println("----------------------------------------------------");
		System.out.println("Entering function :: "+ masterFile + " :: "+name);
		Runtime runtime = Runtime.getRuntime();
		System.out.println("Allocated Memory :: "+runtime.maxMemory() / 1024 /1024);
		System.out.println("Total Memory :: "+ runtime.totalMemory() / 1024 /1024);
		System.out.println("Free Memory :: "+runtime.freeMemory() / 1024 /1024);
		System.out.println("Used Memory :: "+(runtime.totalMemory()-runtime.freeMemory()) / 1024 /1024);
		System.out.println("----------------------------------------------------");
	}

}