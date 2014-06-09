package edu.buffalo.cse562;

import java.io.Serializable;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class Tuple implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final String table;
	private final String[] tuple;

	public Tuple(String table, String tuple) {
		this.tuple = tuple.split("\\|");
		this.table = table;
	}

	public Tuple(String table, String[] tuple) {
		this.tuple = tuple;
		this.table = table;
	}

	public String[] getTupleVaule() {
		return tuple;
	}

	public String getTableName() {
		return table;
	}

	public String getAlias() {

		for (CreateTable ct : Main.ct) {
			if (ct.getTable().getWholeTableName().equalsIgnoreCase(table)) {
				if (ct.getTable().getAlias() != null) {
					return ct.getTable().getAlias();
				} else {
					return ct.getTable().getWholeTableName();
				}
			}
		}
		return null;
	}

	public String getColValue(String col) {

		int count = 0;
		String alias = new String();
		if (col.contains(".")) {

			alias = col.split("\\.", 2)[0];
			col = col.split("\\.")[1];
			if (!getAlias().equalsIgnoreCase(alias)) {
				return null;
			}
		}
		for (CreateTable ct : Main.ct) {

			String t1 = ct.getTable().toString();
			String t2 = table;

			if (t1.equalsIgnoreCase(t2)) {

				for (Object s : ct.getColumnDefinitions()) {
					String col_defn = s.toString();
					if (col_defn.contains(col)) {
						return tuple[count];
					}

					count++;
				}
			}
		}
		return null;
	}

	public String getOsColValue(String col) {
		int count = 0;
		if (col.contains(".")) {
			col = col.split("\\.")[1];
		}

		CreateTable ct = OutputSchema.os;
		String t1 = ct.getTable().toString();
		String t2 = table;

		if (t1.equalsIgnoreCase(t2)) {

			for (Object s : ct.getColumnDefinitions()) {
				String col_defn = s.toString();
				if (col_defn.contains(col)) {

					return tuple[count];
				}
				count++;
			}
		}

		return null;
	}

	public void printTuple() {
		for (String s : tuple) {
			System.out.print(s + "|");
		}
		System.out.println();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (String s : tuple) {
			sb.append(s + "|");
		}
		return sb.substring(0, sb.length() - 2);

	}
}
