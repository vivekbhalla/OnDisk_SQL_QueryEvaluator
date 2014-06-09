package edu.buffalo.cse562;

import java.sql.Date;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ExpressionEvaluator implements ExpressionVisitor,
		SelectItemVisitor, ItemsListVisitor, FromItemVisitor {
	private boolean result = false;
	private long l = Long.MAX_VALUE;
	private String s = null;
	private double d = Double.POSITIVE_INFINITY;
	private Date dt = null;
	private Column c = new Column();
	private Column c2 = null;
	private final List<Expression> or_exp = new ArrayList<Expression>();
	private final List<Expression> where_exp = new ArrayList<Expression>();
	private final LinkedHashSet<String> colList = new LinkedHashSet<String>();

	/**
	 * @return
	 */
	public List<Expression> getExpList() {
		return where_exp;
	}

	/**
	 * @return
	 */
	public List<Expression> or_getExpList() {
		return or_exp;
	}

	/**
	 * @return
	 */
	public boolean getResult() {
		return result;
	}

	/**
	 * @return
	 */
	public long getLongEvaluation() {
		return l;
	}

	/**
	 * @return
	 */
	public double getDoulbeEvaluation() {
		return d;
	}

	/**
	 * @return
	 */
	public Column getColumn() {
		return c;
	}

	/**
	 * @return
	 */
	public Column getUserColumn() {
		return c2;
	}

	/**
	 * @return
	 */
	public LinkedHashSet<String> getColNames() {
		return colList;
	}

	@Override
	public void visit(NullValue nv) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(DateValue dv) {
		dt = dv.getValue();
	}

	@Override
	public void visit(Function fnctn) {
		result = fnctn.isDistinct();
		colList.add(fnctn.getName() + "|" + fnctn.getParameters());
		Column c1 = new Column();
		c1.setColumnName(fnctn.getName() + "|" + fnctn.getParameters());
		c1.setTable(OutputSchema.osTable);
		c = c1;
	}

	@Override
	public void visit(InverseExpression ie) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(JdbcParameter jp) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(DoubleValue dv) {
		d = dv.getValue();
	}

	@Override
	public void visit(LongValue lv) {
		l = lv.getValue();
	}

	@Override
	public void visit(TimeValue tv) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(TimestampValue tv) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Parenthesis prnths) {
		Column c3 = new Column();
		c3.setColumnName(prnths.toString());
		c2 = c3;
		prnths.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue sv) {
		s = sv.getValue();
	}

	@Override
	public void visit(Addition adtn) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;

		adtn.getLeftExpression().accept(this);
		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			adtn.getRightExpression().accept(this);
			if (d != Double.POSITIVE_INFINITY) {
				double right = d;
				d = left + right;
			} else {
				long right = l;
				d = left + right;
			}

		} else {
			long left = l;
			adtn.getRightExpression().accept(this);
			if (d != Double.POSITIVE_INFINITY) {
				double right = d;
				d = left + right;
			} else {
				long right = l;
				l = left + right;
			}
		}

	}

	@Override
	public void visit(Division dvsn) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;

		dvsn.getLeftExpression().accept(this);
		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			dvsn.getRightExpression().accept(this);
			if (d != Double.POSITIVE_INFINITY) {
				double right = d;
				d = left / right;
			} else {
				long right = l;
				d = left / right;
			}

		} else {
			long left = l;
			dvsn.getRightExpression().accept(this);
			if (d != Double.POSITIVE_INFINITY) {
				double right = d;
				d = left / right;
			} else {
				long right = l;
				l = left / right;
			}
		}
	}

	@Override
	public void visit(Multiplication m) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;

		m.getLeftExpression().accept(this);
		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			m.getRightExpression().accept(this);
			if (d != Double.POSITIVE_INFINITY) {
				double right = d;
				d = left * right;
			} else {
				long right = l;
				d = left * right;
			}

		} else {
			long left = l;
			m.getRightExpression().accept(this);
			if (d != Double.POSITIVE_INFINITY) {
				double right = d;
				d = left * right;
			} else {
				long right = l;
				l = left * right;
			}
		}
	}

	@Override
	public void visit(Subtraction s) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;

		s.getLeftExpression().accept(this);

		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			s.getRightExpression().accept(this);
			if (d != Double.POSITIVE_INFINITY) {
				double right = d;
				d = left - right;
			} else {
				long right = l;
				d = left - right;
			}

		} else {
			long left = l;
			s.getRightExpression().accept(this);
			if (d != Double.POSITIVE_INFINITY) {

				double right = d;
				d = left - right;
			} else {
				long right = l;
				l = left - right;
			}
		}
	}

	@Override
	public void visit(AndExpression ae) {
		where_exp.add(ae.getLeftExpression());
		where_exp.add(ae.getRightExpression());

		ae.getLeftExpression().accept(this);
		boolean left = result;
		ae.getRightExpression().accept(this);
		boolean right = result;
		result = left && right;
	}

	@Override
	public void visit(OrExpression oe) {
		or_exp.add(oe.getLeftExpression());
		or_exp.add(oe.getRightExpression());

		oe.getLeftExpression().accept(this);
		boolean left = result;
		oe.getRightExpression().accept(this);
		boolean right = result;
		result = left || right;
	}

	@Override
	public void visit(Between btwn) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(EqualsTo et) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;
		s = null;
		dt = null;

		et.getLeftExpression().accept(this);
		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			et.getRightExpression().accept(this);
			double right = d;
			result = left == right;
		} else if (s != null) {
			String left = s;
			et.getRightExpression().accept(this);
			String right = s;
			result = left.equals(right);
		} else if (l != Long.MAX_VALUE) {
			long left = l;
			et.getRightExpression().accept(this);
			long right = l;
			result = left == right;
		} else if (dt != null) {
			Date left = dt;
			et.getRightExpression().accept(this);
			Date right = dt;
			result = left.compareTo(right) == 0;
		} else {
			et.getRightExpression().accept(this);
		}
	}

	@Override
	public void visit(GreaterThan gt) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;
		s = null;
		dt = null;

		gt.getLeftExpression().accept(this);
		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			gt.getRightExpression().accept(this);
			double right = d;
			result = left > right;
		} else if (l != Long.MAX_VALUE) {
			long left = l;
			gt.getRightExpression().accept(this);
			long right = l;
			result = left > right;
		} else if (dt != null) {
			Date left = dt;
			gt.getRightExpression().accept(this);
			Date right = dt;
			result = left.compareTo(right) > 0;
		} else {
			gt.getRightExpression().accept(this);
		}
	}

	@Override
	public void visit(GreaterThanEquals gte) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;
		s = null;
		dt = null;

		gte.getLeftExpression().accept(this);
		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			gte.getRightExpression().accept(this);
			double right = d;
			result = left >= right;
		} else if (l != Long.MAX_VALUE) {
			long left = l;
			gte.getRightExpression().accept(this);
			long right = l;
			result = left >= right;
		} else if (dt != null) {
			Date left = dt;
			gte.getRightExpression().accept(this);
			Date right = dt;
			result = left.compareTo(right) >= 0;
		} else {
			gte.getRightExpression().accept(this);
		}

	}

	@Override
	public void visit(InExpression ie) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(IsNullExpression ine) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(LikeExpression le) {
		throw new UnsupportedOperationException("Not supported yet.");

		// le.getLeftExpression().accept(this);
		// String left = s;
		// le.getRightExpression().accept(this);
		// String right=s;
		// right = right.replace("%", ".*").replace("_",".");
		// if(Pattern.matches(right, left)){
		// result = true;
		// }else {
		// result = false;
		// }
	}

	@Override
	public void visit(MinorThan mt) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;
		s = null;
		dt = null;

		mt.getLeftExpression().accept(this);
		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			mt.getRightExpression().accept(this);
			double right = d;
			result = left < right;
		} else if (l != Long.MAX_VALUE) {
			long left = l;
			mt.getRightExpression().accept(this);
			long right = l;
			result = left < right;
		} else if (dt != null) {
			Date left = dt;
			mt.getRightExpression().accept(this);
			Date right = dt;
			result = left.compareTo(right) < 0;
		} else {
			mt.getRightExpression().accept(this);
		}
	}

	@Override
	public void visit(MinorThanEquals mte) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;
		s = null;
		dt = null;

		mte.getLeftExpression().accept(this);
		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			mte.getRightExpression().accept(this);
			double right = d;
			result = left <= right;
		} else if (l != Long.MAX_VALUE) {
			long left = l;
			mte.getRightExpression().accept(this);
			long right = l;
			result = left <= right;
		} else if (dt != null) {

			Date left = dt;
			mte.getRightExpression().accept(this);
			Date right = dt;
			result = left.compareTo(right) <= 0;
		} else {
			mte.getRightExpression().accept(this);
		}
	}

	@Override
	public void visit(NotEqualsTo net) {
		d = Double.POSITIVE_INFINITY;
		l = Long.MAX_VALUE;
		s = null;
		dt = null;

		net.getLeftExpression().accept(this);
		if (d != Double.POSITIVE_INFINITY) {
			double left = d;
			net.getRightExpression().accept(this);
			double right = d;
			result = left != right;
		} else if (s != null) {
			String left = s;
			net.getRightExpression().accept(this);
			String right = s;
			result = !left.equals(right);
		} else if (l != Long.MAX_VALUE) {
			long left = l;
			net.getRightExpression().accept(this);
			long right = l;
			result = left != right;
		} else if (dt != null) {
			Date left = dt;
			net.getRightExpression().accept(this);
			Date right = dt;
			result = left.compareTo(right) != 0;
		} else {
			net.getRightExpression().accept(this);
		}
	}

	@Override
	public void visit(Column column) {
		colList.add(column.toString());
		c = column;
	}

	@Override
	public void visit(SubSelect ss) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(CaseExpression ce) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(WhenClause wc) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(ExistsExpression ee) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(AllComparisonExpression ace) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(AnyComparisonExpression ace) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Concat concat) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Matches mtchs) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(BitwiseAnd ba) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(BitwiseOr bo) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(BitwiseXor bx) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(AllColumns arg0) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(AllTableColumns arg0) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(SelectExpressionItem arg0) {
		arg0.getExpression().accept(this);
	}

	@Override
	public void visit(ExpressionList arg0) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Table arg0) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(SubJoin arg0) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}
