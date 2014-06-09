/**
 * 
 */
package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.sf.jsqlparser.statement.create.table.CreateTable;

/**
 * @author Pratik
 * 
 */
public class Metadata {
	private String condition;
	private String lhs;
	private String operator;
	private String rhs;
	private String datatype;
	private int lhscol;
	private int rhscol;
	private final SimpleDateFormat sdf;
	private Date rhsdate;

	public Metadata() {
		lhscol = -1;
		rhscol = -1;
		sdf = new SimpleDateFormat("yyyy-MM-dd");
		rhsdate = null;

	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
		if (condition.contains("=")) {
			if (condition.contains("<=")) {

				operator = "<=";

			} else if (condition.contains(">=")) {
				operator = ">=";
			} else {
				operator = "=";
			}

		} else if (condition.contains("<>")) {
			operator = "<>";
		} else if (condition.contains("<")) {
			operator = "<";
		} else if (condition.contains(">")) {
			operator = ">";
		}

		lhs = condition.split(operator)[0].trim();
		rhs = condition.split(operator)[1].trim();

		datatype = new DataType(lhs).getDatatype();

		lhscol = getColNumber(lhs);
		rhscol = getColNumber(rhs);

		if (datatype.equalsIgnoreCase("date")) {
			if (rhscol == -1) {

				try {
					rhs = rhs.replaceAll("date\\('", "");
					rhs = rhs.replaceAll("DATE\\('", "");
					rhs = rhs.replaceAll("'\\)", "");
					rhsdate = sdf.parse(rhs);

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else if (datatype.equalsIgnoreCase("string")) {
			if (rhscol == -1) {
				rhs = rhs.substring(1, rhs.length() - 1);

			}
		}// parse rhs for double and long

	}

	public String getLhs() {
		return lhs;
	}

	public String getOperator() {
		return operator;
	}

	public String getRhs() {
		return rhs;
	}

	public String getDatatype() {
		return datatype;
	}

	public int getLhscol() {
		return lhscol;
	}

	public int getRhscol() {
		return rhscol;
	}

	public Date getRhsdate() {
		return rhsdate;
	}

	private int getColNumber(String col) {

		String table = null;
		if (col.contains(".")) {
			table = col.split("\\.")[0];
			col = col.split("\\.")[1];
		}
		if (table == null) {
			return -1;
		}

		int count = 0;
		for (CreateTable ct : Main.ct) {

			String t1 = ct.getTable().toString();

			if (t1.equalsIgnoreCase(table)) {

				for (Object s : ct.getColumnDefinitions()) {
					String col_defn = s.toString();

					if (col_defn.contains(col)) {
						return count;

					}

					count++;
				}

			}
		}
		return -1;
	}

	public void printMetadata() {
		System.out.println("CONDITION: " + condition);
		System.out.println("LHS: " + lhs);
		System.out.println("LHS COL: " + lhscol);
		System.out.println("OPERATOR: " + operator);
		System.out.println("RHS: " + rhs);
		System.out.println("RHS COL: " + rhscol);
		System.out.println("DATATYPE: " + datatype);

	}

}
