/**
 * 
 */

package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

/**
 * @author Pratik
 */

public class IndexCreator {

	public List<IndexWrapper> createIndices(List<Expression> where_exp) {
		List<IndexWrapper> index_list = new ArrayList<IndexWrapper>();

		List<IndexWrapper> ordered_index_list = new ArrayList<IndexWrapper>();
		for (Expression w : where_exp) {

			LinkedHashSet<String> colnames;
			List<String> tab_list = new ArrayList<String>();
			List<String> col_list = new ArrayList<String>();
			ExpressionEvaluator e1 = new ExpressionEvaluator();
			w.accept(e1);
			colnames = e1.getColNames();
			for (String tab : colnames) {
				if (tab.contains(".")) {
					tab_list.add(tab.split("\\.")[0]);
					col_list.add(tab.split("\\.")[1]);
				}
			}

			if (tab_list.size() == 2 && w.toString().contains("=")) {
				IndexWrapper iw = new IndexWrapper();
				iw.setE(w);
				iw.setTables(tab_list);
				iw.setColumns(colnames.toArray(new String[0]));
				index_list.add(iw);
			}

		}
		String find = EvaluateStatement.getLargestTable();

		int i = index_list.size();
		while (i > 0) {
			for (IndexWrapper iw : index_list) {
				if (!iw.isFound()) {
					for (String tab : iw.getTables()) {
						if (tab.equalsIgnoreCase(find)) {
							// setting next find
							if (iw.getTables().indexOf(tab) == 0) {
								find = iw.getTables().get(1);
								iw.setIndexOn(iw.getColumns()[1]);
								iw.setName(iw.getTables().get(0));
								iw.setFindValueOf(iw.getColumns()[0]);
							} else {
								find = iw.getTables().get(0);
								iw.setIndexOn(iw.getColumns()[0]);
								iw.setName(iw.getTables().get(1));
								iw.setFindValueOf(iw.getColumns()[1]);
							}
							iw.setFound(true);
							ordered_index_list.add(iw);
							break;
						}

					}
					if (iw.isFound()) {
						break;
					}
				}
			}
			i--;
		}

		for (IndexWrapper iw : ordered_index_list) {
			iw.setHashindex(createHashIndex(iw));
		}

		return ordered_index_list;
	}

	public HashMap<String, List<Tuple>> createHashIndex(IndexWrapper iw) {

		HashMap<String, List<Tuple>> lookup = new HashMap<String, List<Tuple>>();
		BufferedReader br = null;
		String col = iw.getIndexOn().split("\\.")[1];
		String tab = iw.getIndexOn().split("\\.")[0];

		Table tab1 = new Table();

		for (Table t : EvaluateStatement.tables) {

			if (t.getWholeTableName().equalsIgnoreCase(tab)) {
				tab1 = t;
				break;
			} else if (t.getAlias() != null) {

				if (t.getAlias().equalsIgnoreCase(tab)) {
					tab1 = t;
					break;
				}
			}
		}

		try {
			StringBuilder s = new StringBuilder();
			br = new BufferedReader(new FileReader(Main.data_dir + "/"
					+ tab1.getWholeTableName() + ".dat"));
			while (s.append(br.readLine()) != null) {

				Tuple tuple = new Tuple(tab1.getWholeTableName().toLowerCase(),
						s.toString());
				s = null;
				s = new StringBuilder();

				if (lookup.containsKey(tuple.getColValue(col))) {
					lookup.get(tuple.getColValue(col)).add(tuple);
					tuple = null;
				} else {
					List<Tuple> tlist = new ArrayList<Tuple>();
					tlist.add(tuple);
					lookup.put(tuple.getColValue(col), tlist);
					tuple = null;
					// tlist.clear();
				}
			}
			System.gc();
			return lookup;

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

		return null;
	}

	public List<IndexWrapper> createSortedIndices(List<Expression> where_exp) {

		List<IndexWrapper> index_list = new ArrayList<IndexWrapper>();

		List<IndexWrapper> ordered_index_list = new ArrayList<IndexWrapper>();
		for (Expression w : where_exp) {

			LinkedHashSet<String> colnames;
			List<String> tab_list = new ArrayList<String>();
			List<String> col_list = new ArrayList<String>();
			long size[] = new long[2];
			ExpressionEvaluator e1 = new ExpressionEvaluator();
			w.accept(e1);
			colnames = e1.getColNames();
			for (String tab : colnames) {
				if (tab.contains(".")) {
					tab_list.add(tab.split("\\.")[0]);
					col_list.add(tab.split("\\.")[1]);
				}
			}

			if (tab_list.size() == 2 && w.toString().contains("=")) {

				IndexWrapper iw = new IndexWrapper();
				iw.setE(w);
				iw.setTables(tab_list);
				size[0] = EvaluateStatement.getTableSize(tab_list.get(0));
				size[1] = EvaluateStatement.getTableSize(tab_list.get(1));
				iw.setSize(size);
				iw.setColumns(colnames.toArray(new String[0]));
				index_list.add(iw);
			}

		}
		String find = EvaluateStatement.getSmallestTable();

		int i = index_list.size();
		while (i > 0) {
			for (IndexWrapper iw : index_list) {
				if (!iw.isFound()) {
					for (String tab : iw.getTables()) {
						if (tab.equalsIgnoreCase(find)) {
							// setting next find
							if (iw.getTables().indexOf(tab) == 0) {
								find = iw.getTables().get(1);
								iw.setIndexOn(iw.getColumns()[1]);
								iw.setName(iw.getTables().get(0));
								iw.setFindValueOf(iw.getColumns()[0]);
							} else {
								find = iw.getTables().get(0);
								iw.setIndexOn(iw.getColumns()[0]);
								iw.setName(iw.getTables().get(1));
								iw.setFindValueOf(iw.getColumns()[1]);
							}
							iw.setFound(true);
							ordered_index_list.add(iw);
							break;
						}

					}
					if (iw.isFound()) {
						break;
					}
				}
			}
			i--;
		}

		return ordered_index_list;
	}

}
