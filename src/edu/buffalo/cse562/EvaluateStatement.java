package edu.buffalo.cse562;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class EvaluateStatement {
	@SuppressWarnings("unused")
	private final FromItem from;
	private Expression where;

	public static List<Column> groupBy;
	private List<OrderByElement> orderby;
	private final Select select;
	static public List<Table> tables;
	private LinkedHashMap<Integer, List<Tuple>> outstream;
	public static Select oldselect = null;
	static int isSubselect = 0;
	// Extra
	public static HashMap<String, HashSet<String>> GH;

	//
	public EvaluateStatement(Select select) {
		//
		GH = new HashMap<String, HashSet<String>>();
		//
		this.select = select;
		from = null;
		where = null;
		groupBy = null;
		orderby = new ArrayList<OrderByElement>();
		tables = new ArrayList<Table>();
		outstream = new LinkedHashMap<Integer, List<Tuple>>();

	}

	/**
	 * Query Processing is done here, everything from - SUBSELECT, Single or
	 * Multiple Table FROM calculation, GROUP BY, SELECT & ORDER BY
	 * 
	 * @return Final Outstream to be printed
	 * @throws ParseException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public LinkedHashMap<Integer, List<Tuple>> processQuery()
			throws ParseException, IOException {

		PlainSelect plain_select = (PlainSelect) select.getSelectBody();
		SubSelect sub_select = null;

		FromItem from = plain_select.getFromItem();

		TablesNamesFinder t = new TablesNamesFinder();
		from.accept(t);

		// Get Table Names
		tables = t.getTableList(select);
		sub_select = t.getSubSelect();
		if (sub_select == null) {
			if (tables.size() > Main.ct.size()) {
				addSchemaToMainct();
			}
		}
		ConditionEvaluator ce = null;

		// Get SUB SELECT from query if present
		// sub_select = t.getSubSelect();
		List<SelectExpressionItem> selectList = new ArrayList<SelectExpressionItem>();
		selectList = plain_select.getSelectItems();
		new OutputSchema(selectList).createOutputSchema();

		if (sub_select != null) {
			Select s = new Select();
			s.setSelectBody(sub_select.getSelectBody());
			isSubselect = 1;
			outstream = new EvaluateStatement(s).processQuery();
			oldselect = s;
			isSubselect = 2;
			new OutputSchema(selectList).createOutputSchema();
			tables = new ArrayList<Table>();
			Main.ct.add(OutputSchema.os);

		}
		// System.out.println(Main.ct);
		System.gc();
		/* WHERE condition evaluation */

		/* If a Single Table is present directly apply conditions */
		if (tables.size() == 1) {
			outstream = new OperatorFrom(tables, plain_select).caculateFrom();
		}
		/*
		 * If Multiple Tables are present, load them in memory if swap directory
		 * is present and calculate using Nested loop join or else if swap
		 * directory is present using classic Hash Join.
		 */
		else if (tables.size() > 1) {
			ExpressionEvaluator e = new ExpressionEvaluator();
			where = plain_select.getWhere();
			Expression exp = where;
			exp.accept(e);
			List<Expression> where_exp = e.getExpList();

			List<IndexWrapper> ilist = new IndexCreator()
					.createSortedIndices(where_exp);

			ce = new ConditionEvaluator(ilist, where_exp, select);

			ce.binHash();
		}

		// IF OUTSTREAM ALREADY CREATED BY SUB_SELECT, THEN TUPLES ALREADY IN
		// MEMORY, DIRECTLY GROUP BY IF PRESENT
		if (outstream.size() == 0) {
			int key = 0;
			LinkedHashMap<Integer, List<Tuple>> tempoutstream = new LinkedHashMap<Integer, List<Tuple>>();

			for (int j = 0; j < ConditionEvaluator.BINS; j++) {
				if (isSubselect != 2) {
					tempoutstream = ce.addToOutstream(j);

					if (isSubselect == 1) {

						tempoutstream = new OperatorProject(tempoutstream,
								plain_select).calculateProjection();

						writeMap(tempoutstream, j);
						// serialize
						continue;
					}
				} else {
					// deserealize
					tempoutstream = readMap(j);
				}
				groupBy = plain_select.getGroupByColumnReferences();
				if (groupBy != null) {
					tempoutstream = new OperatorGroupBy(tempoutstream, groupBy)
							.calculateGroupBy();

				}
				System.gc();
				/* Calculate SELECT(Projection) */
				int count = 0;
				LinkedHashMap<Integer, List<Tuple>> new_outstream = new LinkedHashMap<Integer, List<Tuple>>();

				/* If GROUP BY is present, calculate projection accordingly */
				if (groupBy != null) {

					for (Integer i : OperatorGroupBy.sel_op.keySet()) {
						LinkedHashMap<Integer, List<Tuple>> instream = OperatorGroupBy.sel_op
								.get(i);

						new_outstream.put(count++, new OperatorProject(
								instream, plain_select)
								.calculateGroupByProjection());

					}
					tempoutstream = new_outstream;
					// new_outstream.clear();
					for (int k : tempoutstream.keySet()) {
						outstream.put(key++, tempoutstream.get(k));
					}
				}

				// tempoutstream.clear();

			}// BINS end

			/* COMBINE GROUP BY OF BINS */
			groupBy = plain_select.getGroupByColumnReferences();
			if (groupBy != null) {
				outstream = new CombineGroup(outstream, groupBy)
						.combineGroupBy();
			}
			System.gc();
			/* Calculate COMBINE SELECT(Projection) of BINS */
			int count = 0;
			LinkedHashMap<Integer, List<Tuple>> new_outstream = new LinkedHashMap<Integer, List<Tuple>>();

			/* If GROUP BY is present, calculate projection accordingly */
			if (groupBy != null) {

				for (Integer i : CombineGroup.sel_op.keySet()) {
					LinkedHashMap<Integer, List<Tuple>> instream = CombineGroup.sel_op
							.get(i);
					new_outstream.put(count++, new OperatorProject(instream,
							plain_select).combineGroupByProjection());

				}
				outstream = new_outstream;
			}
			/* If GROUP BY is not present, calculate projection normally */
			// else {
			//
			// outstream = new OperatorProject(outstream, plain_select)
			// .calculateProjection();
			// }
			System.gc();
		} else {
			// FOR sub_select done, Outstream created
			// for (int i : outstream.keySet()) {
			// for (Tuple T : outstream.get(i)) {
			// T.printTuple();
			// }
			// }

			groupBy = plain_select.getGroupByColumnReferences();
			if (groupBy != null) {
				outstream = new OperatorGroupBy(outstream, groupBy)
						.calculateGroupBy();
			}

			/* Calculate SELECT(Projection) */
			int count = 0;
			LinkedHashMap<Integer, List<Tuple>> new_outstream = new LinkedHashMap<Integer, List<Tuple>>();

			/* If GROUP BY is present, calculate projection accordingly */
			if (groupBy != null) {

				for (Integer i : OperatorGroupBy.sel_op.keySet()) {
					LinkedHashMap<Integer, List<Tuple>> instream = OperatorGroupBy.sel_op
							.get(i);

					new_outstream.put(count++, new OperatorProject(instream,
							plain_select).calculateGroupByProjection());

				}
				outstream = new_outstream;

			}
			/* If GROUP BY is not present, calculate projection normally */
			else {

				outstream = new OperatorProject(outstream, plain_select)
						.calculateProjection();
			}
		}
		/* Calculate ORDER BY */
		orderby = plain_select.getOrderByElements();
		if (orderby != null) {
			/*
			 * To start from last ORDER BY element (Because that is the SQL
			 * Syntax), reverse the ORDER BY list.
			 */
			Collections.reverse(orderby);

			for (OrderByElement o : orderby) {
				outstream = new OperatorOrderBy(outstream, o)
						.calculateOrderBy();
			}
		}
		System.gc();
		return outstream;
	}

	public static String getLargestTable() {
		String ltab = null;
		long maxsize = 0;
		for (Table t : tables) {
			File f1 = new File(Main.data_dir + "/" + t.getWholeTableName()
					+ ".dat");
			if (!f1.exists()) {
				f1 = new File(Main.swap_dir + "/" + t.getWholeTableName()
						+ ".dat");
			}
			if (f1.exists()) {
				if (maxsize < f1.length()) {
					ltab = t.getWholeTableName();
					maxsize = f1.length();
				}
			}
		}
		return ltab;
	}

	public static String getSmallestTable() {
		String ltab = null;
		long minsize = Long.MAX_VALUE;
		for (Table t : tables) {
			File f1 = new File(Main.data_dir + "/" + t.getWholeTableName()
					+ ".dat");
			if (!f1.exists()) {
				f1 = new File(Main.swap_dir + "/" + t.getWholeTableName()
						+ ".dat");
			}

			if (f1.exists()) {
				if (minsize > f1.length()) {
					ltab = t.getWholeTableName();
					minsize = f1.length();
				}
			}
		}
		return ltab;
	}

	public static long getTableSize(String tab) {

		File f1 = new File(Main.data_dir + "/" + tab + ".dat");
		return f1.length() / (1024 * 1024);
	}

	private void addSchemaToMainct() {

		HashMap<String, List<String>> tablecount = new HashMap<String, List<String>>();
		for (Table t : tables) {

			if (tablecount.containsKey(t.getWholeTableName())) {

				tablecount.get(t.getWholeTableName()).add(t.getAlias());

			} else {
				List<String> l = new ArrayList<String>();
				l.add(t.getAlias());
				tablecount.put(t.getWholeTableName(), l);
			}
		}

		List<CreateTable> clist = new ArrayList<CreateTable>();
		List<Integer> remlist = new ArrayList<Integer>();
		List<Integer> tabrem = new ArrayList<Integer>();

		int count = 0;
		for (CreateTable ct : Main.ct) {
			List<String> tabcnt = tablecount.get(ct.getTable()
					.getWholeTableName().toLowerCase());

			if (tabcnt.size() > 1) {
				for (String nt : tabcnt) {
					Table t = new Table();
					CreateTable c = new CreateTable();
					c.setColumnDefinitions(ct.getColumnDefinitions());
					t.setName(nt);

					c.setTable(t);
					clist.add(c);

					tables.add(t);
				}
				remlist.add(count);

				int cnt = 0;
				for (Table t1 : tables) {

					if (t1.getName().equalsIgnoreCase(ct.getTable().getName())) {
						tabrem.add(cnt);
					}
					cnt++;
				}

			}
			count++;
		}

		for (int i : remlist) {
			String copy = Main.ct.get(i).getTable().getWholeTableName();
			File source = new File(Main.data_dir + "/" + copy + ".dat");
			for (String dest : tablecount.get(copy.toLowerCase())) {
				File d = new File(Main.swap_dir + "/" + dest + ".dat");
				try {
					Files.copy(source.toPath(), d.toPath());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}

		int c = 0;
		for (int i : tabrem) {
			tables.remove(i - c);
			c++;
		}

		int c1 = 0;
		for (int i : remlist) {
			Main.ct.remove(i - c1);
			c1++;
		}
		for (CreateTable ct : clist) {
			Main.ct.add(ct);
		}

	}

	@SuppressWarnings("unchecked")
	private LinkedHashMap<Integer, List<Tuple>> readMap(int j) {
		LinkedHashMap<Integer, List<Tuple>> tempoutstream = new LinkedHashMap<Integer, List<Tuple>>();
		try {
			FileInputStream fis = new FileInputStream(Main.swap_dir + "/" + j
					+ ".ser");
			ObjectInputStream ois = new ObjectInputStream(fis);
			tempoutstream = (LinkedHashMap<Integer, List<Tuple>>) ois
					.readObject();
			ois.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return tempoutstream;

	}

	private void writeMap(LinkedHashMap<Integer, List<Tuple>> tempoutstream,
			int j) {

		FileOutputStream fos;
		try {
			fos = new FileOutputStream(Main.swap_dir + "/" + j + ".ser");

			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(tempoutstream);
			oos.flush();
			oos.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
