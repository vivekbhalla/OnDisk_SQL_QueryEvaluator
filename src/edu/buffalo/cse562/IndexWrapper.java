package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.Expression;

public class IndexWrapper {
	private String name = null;
	private HashMap<String, List<Tuple>> hashindex = null;
	private TreeMap<String, Tuple> treeindex = null;
	private Expression e = null;
	private List<String> tables = null;
	private String[] columns = null;
	private boolean found = false;
	private String indexOn = null;
	private String findValueOf = null;
	private long size[];

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public HashMap<String, List<Tuple>> getHashindex() {
		return hashindex;
	}

	public void setHashindex(HashMap<String, List<Tuple>> hashindex) {
		this.hashindex = hashindex;
	}

	public TreeMap<String, Tuple> getTreeindex() {
		return treeindex;
	}

	public void setTreeindex(TreeMap<String, Tuple> treeindex) {
		this.treeindex = treeindex;
	}

	public Expression getE() {
		return e;
	}

	public void setE(Expression e) {
		this.e = e;
	}

	public List<String> getTables() {
		return tables;
	}

	public void setTables(List<String> tables) {
		this.tables = tables;
	}

	public boolean isFound() {
		return found;
	}

	public void setFound(boolean found) {
		this.found = found;
	}

	public String[] getColumns() {
		return columns;
	}

	public void setColumns(String[] columns) {
		this.columns = columns;
	}

	public String getIndexOn() {
		return indexOn;
	}

	public void setIndexOn(String indexOn) {
		this.indexOn = indexOn;
	}

	public String getFindValueOf() {
		return findValueOf;
	}

	public void setFindValueOf(String findValueOf) {
		this.findValueOf = findValueOf;
	}

	public long[] getSize() {
		return size;
	}

	public void setSize(long size[]) {
		this.size = size;
	}
}
