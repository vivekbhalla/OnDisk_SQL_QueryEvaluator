package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

public class OperatorOrderBy {
	private LinkedHashMap<Integer,List<Tuple>> instream;
	private LinkedHashMap<Integer,List<Tuple>> outstream;
	private OrderByElement col;

	public OperatorOrderBy(LinkedHashMap<Integer,List<Tuple>> instream,OrderByElement col) {
		this.instream=instream;
		this.col=col;
		this.outstream=new LinkedHashMap<Integer,List<Tuple>>();
	}

	@SuppressWarnings("unused")
	public LinkedHashMap<Integer, List<Tuple>> calculateOrderBy() {

		long l;
		double d;
		Date dt;
		Time t;
		Timestamp ts;
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
        
		Expression c = col.getExpression();
		String colName = c.toString();
		String dataType = new DataType(colName).getDatatype();

		TreeMap<String, List<Tuple>> orderedString = new TreeMap<String, List<Tuple>>(new Comparator<String>() {  
			@Override public int compare(String o1, String o2) {
				if(col.isAsc())
					if(o1.compareTo(o2)!=0)
						return o1.compareTo(o2);
					else return 1;
				else
					if(o2.compareTo(o1)!=0)
						return o2.compareTo(o1);
					else
						return 1;
			}});  

		TreeMap<Long, List<Tuple>> orderedLong = new TreeMap<Long, List<Tuple>>(new Comparator<Long>() {  
			@Override public int compare(Long o1, Long o2) {  
				if(col.isAsc())
					if(o1.compareTo(o2)!=0)
						return o1.compareTo(o2);
					else return 1;
				else
					if(o2.compareTo(o1)!=0)
						return o2.compareTo(o1);
					else
						return 1;		}}); 

		TreeMap<Double, List<Tuple>> orderedDouble = new TreeMap<Double, List<Tuple>>(new Comparator<Double>() {  
			@Override public int compare(Double o1, Double o2) {  
				if(col.isAsc())
					if(o1.compareTo(o2)!=0)
						return o1.compareTo(o2);
					else return 1;
				else
					if(o2.compareTo(o1)!=0)
						return o2.compareTo(o1);
					else
						return 1;			}});

		TreeMap<Date, List<Tuple>> orderedDate = new TreeMap<Date, List<Tuple>>(new Comparator<Date>() {  
			@Override public int compare(Date o1, Date o2) {  
				if(col.isAsc())
					if(o1.compareTo(o2)!=0)
						return o1.compareTo(o2);
					else return 1;
				else
					if(o2.compareTo(o1)!=0)
						return o2.compareTo(o1);
					else
						return 1;			}});

		TreeMap<Time, List<Tuple>> orderedTime = new TreeMap<Time, List<Tuple>>(new Comparator<Time>() {  
			@Override public int compare(Time o1, Time o2) {  
				if(col.isAsc())
					if(o1.compareTo(o2)!=0)
						return o1.compareTo(o2);
					else return 1;
				else
					if(o2.compareTo(o1)!=0)
						return o2.compareTo(o1);
					else
						return 1;
				}});  

		TreeMap<Timestamp, List<Tuple>> orderedTimestamp = new TreeMap<Timestamp, List<Tuple>>(new Comparator<Timestamp>() {  
			@Override public int compare(Timestamp o1, Timestamp o2) {  
				if(col.isAsc())
					if(o1.compareTo(o2)!=0)
						return o1.compareTo(o2);
					else return 1;
				else
					if(o2.compareTo(o1)!=0)
						return o2.compareTo(o1);
					else
						return 1;
			}}); 
		//System.out.println(instream);

		for(int k: instream.keySet()){
			for(Tuple tup : instream.get(k)){

				String val = tup.getOsColValue(colName);
		
				if(val!=null){
					if(dataType.equalsIgnoreCase("long")){
						l = Long.parseLong(val);
						orderedLong.put(l, instream.get(k));
						break;
					}else if(dataType.equalsIgnoreCase("string")){

						orderedString.put(val, instream.get(k));
						break;
					}else if(dataType.equalsIgnoreCase("date")){
						try{
							dt =sdf.parse(val);	

							orderedDate.put(dt, instream.get(k));
							break;
						}catch(ParseException e){
							e.printStackTrace();
						}
						
						
					}else if(dataType.equalsIgnoreCase("double")){
						d = Double.parseDouble(val);
						orderedDouble.put(d, instream.get(k));
						break;
					}else if(dataType.equalsIgnoreCase("time")){

					}else if(dataType.equalsIgnoreCase("timestamp")){

					}
				}
			}

		}

				
		if(!orderedLong.isEmpty()){
			int key = 0;
			for(List<Tuple> lg : orderedLong.values()){
				outstream.put(key, lg);
				key++;
			}
		}else if(!orderedDouble.isEmpty()){
			int key = 0;
			for(List<Tuple> db : orderedDouble.values()){
				outstream.put(key,db);
				key++;
			}
		}else if(!orderedString.isEmpty()){
			int key = 0;
			for(List<Tuple> st : orderedString.values()){
				outstream.put(key, st);
				key++;
			}
		}else if(!orderedDate.isEmpty()){
			int key = 0;
			for(List<Tuple> da : orderedDate.values()){
				outstream.put(key,da);
				key++;
			}
		}else if(!orderedTime.isEmpty()){
			int key = 0;
			for(List<Tuple> tm : orderedTime.values()){
				outstream.put(key,tm);
				key++;
			}
		}else if(!orderedTimestamp.isEmpty()){
			int key = 0;
			for(List<Tuple> tsp : orderedTimestamp.values()){
				outstream.put(key,tsp);
				key++;
			}
		}

		//Print Sorted Map
		//      for(Integer i : outstream.keySet()){
		//          System.out.println(i);
		//          for(Tuple t1: outstream.get(i)){
		//              for(String s:t1.getTupleVaule()){
		//                  System.out.print(s+ " ");
		//              }
		//          }
		//          System.out.println();
		//      }

		return outstream;
	}
}