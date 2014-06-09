package edu.buffalo.cse562;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;

public class OperatorGroupBy {
	private final LinkedHashMap<Integer, List<Tuple>> instream;
	private final LinkedHashMap<Integer, List<Tuple>> outstream;
	private final List<Column> col;
	public static LinkedHashMap<Integer, LinkedHashMap<Integer, List<Tuple>>> sel_op;

	public OperatorGroupBy(LinkedHashMap<Integer, List<Tuple>> instream,
			List<Column> col) {
		this.instream = instream;
		this.col = col;
		outstream = new LinkedHashMap<Integer, List<Tuple>>();
		sel_op = new LinkedHashMap<Integer, LinkedHashMap<Integer, List<Tuple>>>();

	}

	public LinkedHashMap<Integer, List<Tuple>> calculateGroupBy() {

		LinkedHashMap<String, LinkedList<List<Tuple>>> grouped = new LinkedHashMap<String, LinkedList<List<Tuple>>>();

		for (int k : instream.keySet()) {
			StringBuilder key = new StringBuilder();
			for (Column colName : col) {
				for (Tuple tup : instream.get(k)) {

					String val = tup.getColValue(colName.toString());
					if (val != null) {
						key.append(val);
						break;
					}

				}
			}
			if (grouped.containsKey(key.toString())) {
				grouped.get(key.toString()).add(instream.get(k));
			} else {
				LinkedList<List<Tuple>> temp = new LinkedList<List<Tuple>>();
				temp.add(instream.get(k));
				grouped.put(key.toString(), temp);
			}

		}

		int count = 0;
		int count2 = 0;
		int count3 = 0;
		LinkedList<List<Tuple>> traverse = new LinkedList<List<Tuple>>();
		for (String sb : grouped.keySet()) {
			traverse = grouped.get(sb);
			LinkedHashMap<Integer, List<Tuple>> temp = new LinkedHashMap<Integer, List<Tuple>>();
			for (List<Tuple> t : traverse) {
				outstream.put(count++, t);
				temp.put(count2++, t);
			}
			count2 = 0;

			sel_op.put(count3++, temp);
		}
		// Print Sorted Map
		// for(Integer i : outstream.keySet()){
		// System.out.println(i);
		// for(Tuple t1: outstream.get(i)){
		// for(String s:t1.getTupleVaule()){
		// System.out.print(s+ "|");
		// }
		// }
		// System.out.println();
		// }
		return outstream;
	}

}
