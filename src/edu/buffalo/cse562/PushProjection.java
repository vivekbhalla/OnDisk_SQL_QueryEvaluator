package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class PushProjection {

	private final List<Object> newColDef;
	private final Select select;
	private final List<Integer> projectOffset = new ArrayList<Integer>();

	public PushProjection(Select select) {
		this.select = select;
		newColDef = new ArrayList<Object>();
	}

	@SuppressWarnings("unchecked")
	public List<Object> coldefs(CreateTable ct) {

		PlainSelect plain_select = (PlainSelect) select.getSelectBody();
		List<SelectExpressionItem> selectList = new ArrayList<SelectExpressionItem>();

		selectList = plain_select.getSelectItems();
		List<String> colList = new LinkedList<String>();
		for (SelectExpressionItem s : selectList) {

			Expression exp = s.getExpression();
			ExpressionEvaluator ee = new ExpressionEvaluator();
			exp.accept(ee);
			if (ee.getUserColumn() != null) {
				String tempexp = "(" + exp.toString() + ")";
				colList.add(tempexp);

			} else {
				colList.add(ee.getColumn().getWholeColumnName().toString());
			}
		}
		int count = 0;
		for (Object o : ct.getColumnDefinitions()) {
			String temp = o.toString();
			String tabcol = ct.getTable().getWholeTableName().toLowerCase()
					+ "." + temp.split(" ")[0];
			for (String s : colList) {
				if (s.contains(tabcol)) {
					newColDef.add(o);
					projectOffset.add(count);
					break;
				}
			}
			count++;
		}
		return newColDef;

	}

	public List<Integer> getProjectOffset() {
		return projectOffset;
	}

}
