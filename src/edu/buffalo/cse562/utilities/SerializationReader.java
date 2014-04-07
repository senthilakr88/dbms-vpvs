package edu.buffalo.cse562.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import edu.buffalo.cse562.physicalPlan.Datum;

public class SerializationReader {

	
	public static boolean readFile(ObjectInputStream br) {
		try {

			// System.out.println("br.available :: "+br.available());
			ArrayList<Datum[]> datum = (ArrayList<Datum[]>) br.readObject();
			if (datum != null) {
				printTuple(datum);
				return true;
			} else {
				// System.out.println("Closing Buffer");
				br.close();

			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;

	}

	// used to create an reader object stream
	public static ObjectInputStream readOS(String fileName) {
		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream(new FileInputStream(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return in;
	}

	
	public static void main(String[] args) {
		String FileName = "orders.ser";
		String swapDir = "C:\\Pals\\Code\\GitHub\\Database\\team13\\swapDir\\orders\\";
		
//		System.out.println(this.swapDir);
		if (!(new File(swapDir).exists())) {
			swapDir = new File("").getAbsolutePath() + File.separator + swapDir;
		}
		
		ObjectInputStream ois = readOS(swapDir+File.separator+FileName);
		readFile(ois);
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

	public static void printTuple(Datum[] row) {
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

	public static void printTuple(ArrayList<Datum[]> row) {
		if (row != null) {
			Iterator ite = row.iterator();
			while (ite.hasNext()) {
				printTuple((Datum[]) ite.next());
				System.out.print(" , ");
			}
			System.out.println();
		}
	}


}
