package edu.buffalo.cse562.physicalPlan;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.physicalPlan.Datum.dString;
import edu.buffalo.cse562.sql.expression.evaluator.CalcTools;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class OrderByOperator implements Operator {
	Operator oper;
	Datum[] t1,t2;
	List<Datum[]> list1;
	List<OrderByElement> elements;
	//Boolean firstEntry;

	public OrderByOperator(Operator oper,List<OrderByElement> elements) {
		this.oper=oper;
		this.elements=elements;
	}

	@Override
	public void resetStream() {
		// TODO Auto-generated method stu
		
	}

	@Override
	public Datum[] readOneTuple() {
		t1=oper.readOneTuple();
		list1=null;
		Iterator<OrderByElement> iter=elements.iterator();
		Datum t2[]=null;
		boolean arr[]=null;
		int i=0;
		while(iter.hasNext())
		{
			OrderByElement ele=iter.next();
			Expression exe=ele.getExpression();
			arr[i]=ele.isAsc();
			CalcTools calc = new CalcTools(t1); 
			Object ob = calc.getResult();
			t2=null;
			i++;
		}
		list1.add(t2);//Add all
		
		Iterator<Datum[]> iter1=list1.iterator();
		while(iter1.hasNext())
		{
		 Datum[] t=iter1.next();
		
			
		
		
		while(list1!=null)
		{
			
		Datum[] d=list1.remove(0);
		if(d[0] instanceof dString)
		{
			dString str=(dString)d[0];
			String s=str.getValue();
			if()
		}
				
	/*	if(arr[0])
		{
		Collections.sort(list1, new Mysorter());
		}
		else
		{
		Collections.sort(list1, new Myreversesorter());
		
		}  */
		while(list1!=null)
		{
			
		}
		return null;
	}

}
