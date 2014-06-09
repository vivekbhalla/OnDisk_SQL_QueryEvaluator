/**
 * 
 */

package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author Pratik
 */
public class TableExpInfo {

	private HashSet<String> col;
	private String exp;
	private int[] cntList;
	private String[] datatype;
	// this only works for query with single or list
	private ArrayList<Metadata>[] andOrList;

	public HashSet<String> getCol() {
		return col;
	}

	public void setCol(HashSet<String> col) {
		this.col = col;
	}

	public String getExp() {
		return exp;
	}

	public void setExp(String exp) {
		this.exp = exp;
	}

	public int[] getCntList() {
		return cntList;
	}

	public void setCntList() {
		cntList = new int[col.size()];
		int count = 0;
		for (String s : col) {
			cntList[count++] = ConditionEvaluator.getColNumber(s,
					s.split("\\.")[0]);
		}

	}

	public String[] getDatatype() {
		return datatype;
	}

	public void setDatatype() {
		datatype = new String[col.size()];
		int count = 0;
		for (String s : col) {
			datatype[count++] = new DataType(s).getDatatype();
			;
		}
	}

	public ArrayList<Metadata>[] getAndOrList() {
		return andOrList;
	}

	public void setAndOrList(ArrayList<Metadata>[] andOrList) {
		this.andOrList = andOrList;
	}
}
