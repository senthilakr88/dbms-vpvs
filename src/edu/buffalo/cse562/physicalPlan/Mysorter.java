package edu.buffalo.cse562.physicalPlan;

import java.util.Comparator;

import edu.buffalo.cse562.physicalPlan.Datum.dLong;
import edu.buffalo.cse562.physicalPlan.Datum.dString;
import edu.buffalo.cse562.physicalPlan.Datum.dString.dDate;

public class Mysorter implements Comparator<Datum[]> {

	@Override
	public int compare(Datum[] t1, Datum[] t2) {
		int length1 = t1.length;
		int length2 = t2.length;
		int i = 0;
	
		
		if (t1[i] instanceof dLong) {
				dLong d1 = (dLong) t1[i];
				dLong d2 = (dLong) t2[i];
				if (d1.getValue() > d2.getValue())
					return -1;
				else if (d1.getValue() < d2.getValue())
					return 1;
				else 
				    return 0;		
		
		     
			}
			else if (t1[i] instanceof dString) {
				dString d1 = (dString) t1[i];
				dString d2 = (dString) t2[i];
				 if((d1.getValue().compareToIgnoreCase(d2.getValue()))==-1)
					return -1;
				else if((d1.getValue().compareToIgnoreCase(d2.getValue()))==1)
					return 1;
				else 
				
				  return 0;
				
		
		      }
			else if(t1[i] instanceof dDate) {
					dDate d1 = (dDate) t1[i];
					dDate d2 = (dDate) t2[i];
					 if((d1.getValue().compareTo(d2.getValue()))==-1)
						return -1;
					else if((d1.getValue().compareTo(d2.getValue()))==1)
						return 1;
					else 
					{	
						return 0;
			        }
			}
			
         	else
		        return 0;

	     }

	}

