package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class OperatorFrom {

	private static LinkedHashMap<Integer, List<Tuple>> outstream;
	private static int count;
	private final List<Table> tables;
	private static PlainSelect select;
	private LinkedHashSet<String> colnames;
	private Expression where;
	private static OperatorWhere wh;

	public OperatorFrom(List<Table> tables, PlainSelect sel) {
		this.tables = tables;
		select = sel;

		outstream = new LinkedHashMap<Integer, List<Tuple>>();
		count = 0;

	}

	public LinkedHashMap<Integer, List<Tuple>> caculateFrom() {

		where = select.getWhere();
		Expression exp = where;
		if (where != null) {
			ExpressionEvaluator e = new ExpressionEvaluator();
			exp.accept(e);
			colnames = e.getColNames();
		}
		wh = new OperatorWhere(colnames, where);

		LinkedList<LinkedHashMap<Integer, List<Tuple>>> tablesInMemory = new LinkedList<LinkedHashMap<Integer, List<Tuple>>>();

		for (Table t : tables) {

			String dat_file = Main.data_dir + "/" + t.getWholeTableName()
					+ ".dat";
			tablesInMemory.add(readTable(dat_file, t));
		}

		generateCartesianProduct(tablesInMemory, new ArrayList<Tuple>());
		// System.out.println(outstream.size());
		// System.out.println(outstream);
		// System.err.println("1- "+outstream.size());
		return outstream;

	}

	public LinkedHashMap<Integer, List<Tuple>> readTable(String dat_file,
			Table table) {

		LinkedHashMap<Integer, List<Tuple>> output = new LinkedHashMap<Integer, List<Tuple>>();
		BufferedReader br = null;
		try {
			String s;
			br = new BufferedReader(new FileReader(dat_file));

			int count = 0;
			while ((s = br.readLine()) != null) {

				Tuple tuple = new Tuple(
						table.getWholeTableName().toLowerCase(), s);
				List<Tuple> tlist = new ArrayList<Tuple>();
				tlist.add(tuple);
				output.put(count, tlist);
				count++;

			}

			return output;

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}

		}

		return output;
	}

	public static void generateCartesianProduct(
			LinkedList<LinkedHashMap<Integer, List<Tuple>>> outerlist,
			List<Tuple> outputstream) {

		LinkedHashMap<Integer, List<Tuple>> map = outerlist.get(0);

		for (Integer i : map.keySet()) {
			List<Tuple> tlist = map.get(i);
			LinkedList<LinkedHashMap<Integer, List<Tuple>>> new_outerlist = new LinkedList<LinkedHashMap<Integer, List<Tuple>>>(
					outerlist);
			new_outerlist.remove(map);

			if (outerlist.size() > 1) {

				outputstream.add(tlist.get(0));
				generateCartesianProduct(new_outerlist, outputstream);
				outputstream.remove(tlist.get(0));

			} else {
				outputstream.add(tlist.get(0));
				if (wh.calculateCondition(outputstream)) {
					List<Tuple> t = new ArrayList<Tuple>(outputstream);
					outstream.put(count, t);
					// System.out.println(outstream);
					count++;
					// System.out.println(count);
				}
				outputstream.remove(tlist.get(0));

			}

		}

	}

}