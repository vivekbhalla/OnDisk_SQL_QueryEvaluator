
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.util.ArrayList;
import java.util.List;

public class OutputSchema {
    private List<SelectExpressionItem> colnames = new ArrayList<SelectExpressionItem>();
    public static CreateTable os;
    public static Table osTable;
    private List<SelectExpressionItem> oldcolnames = new ArrayList<SelectExpressionItem>();

    public OutputSchema(List<SelectExpressionItem> colnames) {
        this.colnames = colnames;
        os = new CreateTable();
        osTable = new Table();
    }

    @SuppressWarnings("unchecked")
    public void createOutputSchema() throws ParseException {
        if (EvaluateStatement.oldselect == null) {

            List<Column> clist = new ArrayList<Column>();
            for (SelectExpressionItem e : colnames) {
                if (e.getAlias() == null) {

                    Expression exp = e.getExpression();
                    ExpressionEvaluator ev = new ExpressionEvaluator();
                    exp.accept(ev);
                    clist.add(ev.getColumn());

                } else {
                    Column c = new Column();
                    c.setTable(osTable);
                    c.setColumnName(e.getAlias());

                    clist.add(c);
                }
            }

            osTable.setName("ostable");
            os.setTable(osTable);
            os.setColumnDefinitions(clist);
        } else {
            PlainSelect olps = (PlainSelect) EvaluateStatement.oldselect
                    .getSelectBody();
            oldcolnames = olps.getSelectItems();
            List<ColumnDefinition> coldefs = new ArrayList<ColumnDefinition>();

            for (SelectExpressionItem e : colnames) {

                Expression exp = e.getExpression();
                ExpressionEvaluator ev = new ExpressionEvaluator();
                exp.accept(ev);
                coldefs.add(getdef(ev.getColumn()));
            }

            osTable.setName("ostable");
            os.setTable(osTable);
            os.setColumnDefinitions(coldefs);

        }

    }

    private ColumnDefinition getdef(Column column) {
        ColumnDefinition newcol = new ColumnDefinition();

        Column c = null;

        if (column.getColumnName().contains("|")) {
            newcol.setColumnName(column.getColumnName());
            return newcol;
        }

        for (SelectExpressionItem e : oldcolnames) {
            if (e.getAlias() != null) {

                if (e.getAlias().equalsIgnoreCase(column.getColumnName())) {
                    Expression exp = e.getExpression();
                    ExpressionEvaluator ev = new ExpressionEvaluator();
                    exp.accept(ev);
                    c = ev.getColumn();
                    break;
                }
            }
        }
        String col;
        ColDataType dt = new ColDataType();
        String datatype;
        for (CreateTable ct : Main.ct) {
            for (Object cd : ct.getColumnDefinitions()) {
                String temp = cd.toString();
                col = temp.split(" ")[0];
                datatype = temp.split(" ")[1];
                if (c.getColumnName().contains(col)) {
                    dt.setDataType(datatype);
                    newcol.setColDataType(dt);
                    newcol.setColumnName(column.getWholeColumnName());
                    break;
                }
            }
        }
        return newcol;
    }
}
