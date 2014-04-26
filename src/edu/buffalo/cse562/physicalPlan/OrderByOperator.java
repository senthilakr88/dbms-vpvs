package edu.buffalo.cse562.physicalPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.structure.Datum;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class OrderByOperator implements Operator{
	Operator oper;
	Datum[] t1, t2;
	List<Datum[]> listDatum;
	List<OrderByElement> elements;
	int index;

	// Boolean firstEntry;

	public OrderByOperator(List<OrderByElement> elements, List<Datum[]> listDatum) {
		this.elements = elements;
		this.listDatum = listDatum;
		this.index = 0;
		sort();
//		print();
	}

	public List<Datum[]> getListDatum() {
		return listDatum;
	}

	public void setListDatum(List<Datum[]> listDatum) {
		this.listDatum = listDatum;
	}
	
	public void addTuple(Datum[] Tuple) {
		listDatum.add(Tuple);
	}
	
	public void sort() {
		Collections.sort(listDatum, new Mysorter(elements));
	}

	public void print() {
		Iterator<Datum[]> ite = listDatum.iterator();
		
		while(ite.hasNext()) {
		Boolean first = true;
		Datum[] row = ite.next();
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
	}

	@Override
	public void resetStream() {
		oper.resetStream();		
	}

	@Override
	public Datum[] readOneTuple() {
		Datum[] temp = null;
		if(listDatum!=null && index != listDatum.size()) {
			temp = listDatum.get(index);
			++index;
		}
		return temp;
	}

	@Override
	public void resetTupleMapping() {
		// TODO Auto-generated method stub
		
	}

}
