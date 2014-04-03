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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
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
	BufferedReader reader, reader1;
	ArrayList<Datum[]>[] buffer;
	Integer bufferMaxSize;
	HashMap<Integer, ArrayList<Datum[]>> hmap;
	ArrayList<Datum[]> result = null;
	BufferedReader[] buffread = null;
	List<OrderByElement> elements;
	List<Integer> index;
	List<Boolean> asc;
	Boolean first;

	// Constructor of ExternalSort
	public ExternalSort(Operator oper, List elements, String swapDir) {
		this.oper = oper;
		this.elements = elements;
		this.swapDir = swapDir;
		this.bufferMaxSize = 300;
		this.first = true;
		hmap = new HashMap<Integer, ArrayList<Datum[]>>();
		index = new ArrayList<Integer>();
		asc = new ArrayList<Boolean>();
		result = new ArrayList<Datum[]>();
		
	}

	// Used to get an reader for a dat file
	// Not used any more.
	public BufferedReader createinputreader(String s) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(s));

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}
		return reader;
	}

	// Used to get an writer for a data file
	// Not used any more
	public PrintWriter createoutstream(String s) {
		PrintWriter out = null;
		try {
			File file = new File(s);
			if (!file.exists())
				file.createNewFile();
			out = new PrintWriter(new BufferedWriter(new FileWriter(s, true)));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return out;

	}

	// To write data to a data file
	// Not used any more
	public void writedata(PrintWriter out, int i) {
		if (i < 0)
			out.write(result.toString());
		else
			out.write(buffer[i].toString());

	}

	// To read data to a data file
	// Not used any more
	boolean readfile(BufferedReader br, int i, int depth) {

		// String index = Integer.toString(i) + Integer.toString(depth);
		// int indexint = Integer.parseInt(index);

		Datum[] oneTupleFromDat = null;
		String line = null;
		int count = 0;
		try {

			while ((line = br.readLine()) != null && count < bufferMaxSize) {
				String[] singleTableElement = line.split("\\|");

				// oneTupleFromDat = new
				// Tuple(convertType(singleTableElement));
				// oneTupleFromDat = convertType(singleTableElement);
				buffer[i].add(oneTupleFromDat);
				count++;
			}

			// hmap.put(i,buffer[i]);
		} catch (IOException e) {

			e.printStackTrace();
		}
		if (buffer[i].size() > 0)
			return true;
		else {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return false;
		}
	}

	// Used to create a writer for an object stream
	public void writedata(ObjectOutputStream out, int i) {
		try {
			if (i < 0)
				out.writeObject(result);
			else
				out.writeObject(buffer[i]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//used to create a writer object stream
	public ObjectOutputStream writeOS(String fileName) {
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream(new FileOutputStream(fileName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return out;
	}

	//used to create an reader object stream
	public ObjectInputStream readOS(String fileName) {
		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream(new FileInputStream(fileName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return in;
	}

	boolean readfile(int i, int depth) {
		Datum[] oneTupleFromDat = null;
		int count = 0;
		oneTupleFromDat = oper.readOneTuple();
		if (first && oneTupleFromDat != null) {
			computeIndex(oneTupleFromDat);
			first = false;
		}
		buffer[i] = new ArrayList<Datum[]>();
		while (oneTupleFromDat != null) {
			buffer[i].add(oneTupleFromDat);
			count++;
			if (count < bufferMaxSize) {
				oneTupleFromDat = oper.readOneTuple();
			} else {
				break;
			}
		}
		if (buffer[i].size() > 0)
			return true;
		else {
			return false;
		}
	}

	//This function return sort the buffer in first pass
	int readpage() {
		int i = 0;
		int runs = 0;
		String s1 = null;
		String s = null, index = null;
		try {
			//This function gets the first buffer
			while (readfile(i, 1)) {
				sortdata(i);
				s = swapDir + "buffer[";
				index = Integer.toString(i) + Integer.toString(1);
				s = s + index + "].ser";
				ObjectOutputStream out = writeOS(s);
				writedata(out, i);
				out.close();
				buffer[i] = null;
				i++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return i;
	}

	@SuppressWarnings("unchecked")
	void sortfile() {
		// Initial sort, completed phase 1
		int runs = readpage();
		int count = 0;
		String line = null;
		Datum[] oneTupleFromDat = null;
		BufferedReader[] reader = null;
		ObjectInputStream in;
		int i = 0, k = 0, runcurrent = 0, depth = 1, filenumber = 0;
		boolean check = true;
		try {
			while ((runs - k) > 0 && check) {
				if (runs > 5) {
					k = 5;
				} else {
					k = runs;
				}

				count = k;
				while (count > 0) {
					String s = swapDir + "buffer[";
					String index = Integer.toString(i)
							+ Integer.toString(depth);
					s = s + index + "].ser";
					// buffread[i] = createinputreader(s);
					// readfile(buffread[i], i, depth);
					in = readOS(s);
					buffer[i] = (ArrayList<Datum[]>) in.readObject();
					i++;
					count--;
					in.close();
				}
				if (((runs - k) == 0) && (runcurrent == 0)) {
					secondsort(i - k, k, filenumber, depth - 30000);
				} else {
					secondsort(i - k, k, filenumber, depth);
				}
				filenumber++;
				runcurrent++;

				if ((runs - k) == 0) {
					if (runcurrent == 1)
						check = false;
					runs = runcurrent;
					runcurrent = 0;
					filenumber = 0;
					depth = depth + 1;
					k = 0;
					i = 0;
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	void secondsort(int start, int count, int filenumber, int depth) {
		int runcount = 0;
		ObjectOutputStream out = null;
		Map<String, Boolean> map1 = new HashMap<String, Boolean>();
		List<ArrayList<Datum[]>> group1 = new ArrayList<ArrayList<Datum[]>>();
		int i = 0, counter = 1;
		int numberofemptylists = 0;
		String depthstr = Integer.toString(depth);

		while (i < count) {
			group1.add(buffer[Integer.parseInt(Integer.toString(start))]);
			start++;
			i++;
		}
		start = start - count;

		ArrayList<Datum[]> lowest = new ArrayList<Datum[]>();
		int capacity = 1000;
		Iterator<ArrayList<Datum[]>> iter = group1.iterator();
		boolean first;
		ArrayList<Datum[]> list1 = null;
		boolean check = true;
		try {
			while (result.size() < capacity && check) { // while we still have
														// something to add
				first = true;
				int k = 0;
				for (int j = start; j < count; j++) {
					list1 = group1.get(j);
					if (list1.size() != 0) {
						if (first) {
							lowest = list1;
							first = false;
						} else if (compare(list1.get(0), lowest.get(0)) <= 0) {
							lowest = list1;
						}
					} else {
						boolean s = false;
						if (depth < 0)
							s = readfile(buffread[j], j, depth + 30000);
						s = readfile(buffread[j], j, depth);
						if (s == true) {
							j--;
						} else {

							boolean ch = false;
							if (depth < 0) {
								depth = depth + 30000;
								ch = true;
							}
							if (!map1.containsKey(Integer.toString(j)
									+ Integer.toString(depth))) {
								map1.put(
										Integer.toString(j)
												+ Integer.toString(depth), true);
								numberofemptylists++;
								if (numberofemptylists == count) {
									if (ch == true)
										filenumber = -1;
									writedata(out, -2);
									result.clear();
									check = false;
									map1.clear();
									out.close();
								}
							}
							if (ch == true) {
								ch = false;
								depth = depth - 30000;
							}
						}

					}
				}
				result.add(lowest.get(0));
				lowest.remove(0);
				if (result.size() == capacity) {

					if (counter == 1) {
						String s = null;
						String index = null;
						if (depth < 0)
							s = "vfgt";
						else {
							s = swapDir + "buffer[";
							index = Integer.toString(i)
									+ Integer.toString(depth + 1);
							s = s + index + "].ser";
						}

						out = writeOS(s);
						counter = 0;
					}

					writedata(out, -2);
					result.clear();
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	void sortdata(int i) {
		Collections.sort(buffer[i], new Mysorter(elements));
	}

	public void computeIndex(Datum[] tuple) {
		int ind;
		Column col;
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
	}

	public int compare(Datum[] t1, Datum[] t2) {
		Iterator iter = elements.iterator();
		CalcTools calc1 = null, calc2 = null;
		int comparison = -2;
		int k = 0;
		int indexSize = index.size();
		int ind;
		while (k < indexSize) {
			ind = index.get(k);
			comparison = TupleStruct.getCompareValue((Object) t1[ind],
					(Object) t2[ind], asc.get(k));

			if (comparison != 0) {
				// System.out.println("In :: " + calc1.getResult() + " : " +
				// calc2.getResult() + " : " + comparison);
				return comparison;
			}
		}
		// System.out.println("Out :: " + calc1.getResult() + " : " +
		// calc2.getResult() + " : " + comparison);
		return comparison;

	}

	@Override
	public void resetStream() {
		oper.resetStream();
	}

	@Override
	public Datum[] readOneTuple() {
		return null;
	}

}
