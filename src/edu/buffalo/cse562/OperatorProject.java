
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

public class OperatorProject {
    private final LinkedHashMap<Integer, List<Tuple>> instream;
    private final PlainSelect select;
    private final LinkedHashMap<Integer, List<Tuple>> outstream;
    private List<SelectExpressionItem> selectList;
    public List<Expression> colnames;
    private double d;
    private final long l;

    public OperatorProject(LinkedHashMap<Integer, List<Tuple>> instream,
            PlainSelect select) {
        this.instream = instream;
        this.select = select;
        outstream = new LinkedHashMap<Integer, List<Tuple>>();
        selectList = new ArrayList<SelectExpressionItem>();
        colnames = new ArrayList<Expression>();
        d = Double.POSITIVE_INFINITY;
        l = Long.MAX_VALUE;
    }

    @SuppressWarnings("unchecked")
    public LinkedHashMap<Integer, List<Tuple>> calculateProjection()
            throws ParseException {
        List<Tuple> new_tlist;
        StringBuilder sb;
        selectList = select.getSelectItems();
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
        for (String col : colList) {
            if (col.contains("|")) {
                new_tlist = calculateGroupByProjection();
                outstream.put(0, new_tlist);
                return outstream;
            }

        }
        int count = 0;
        LinkedHashSet<String> colnames2 = new LinkedHashSet<String>();
        for (String col : colList) {
            if (col.contains("(")) {

                Expression exp4 = Main.parseArithematicExpression(col);
                ExpressionEvaluator eee1 = new ExpressionEvaluator();
                exp4.accept(eee1);
                colnames2 = eee1.getColNames();
            }
        }
        for (Integer i : instream.keySet()) {

            sb = new StringBuilder();
            new_tlist = new ArrayList<Tuple>();
            for (String col : colList) {
                for (Tuple t : instream.get(i)) {
                    if (col.contains("(")) {

                        // calculate(t, col, colnames2);
                        calculate(t, col);
                        if (d != Double.POSITIVE_INFINITY) {
                            sb.append(d + "|");
                            break;
                        }

                    } else {
                        String val = t.getColValue(col);

                        if (val != null) {
                            sb.append(val + "|");
                            break;
                        }
                    }
                }
            }

            String t = sb.substring(0, sb.length() - 1);
            Tuple tuple = new Tuple(OutputSchema.osTable.getWholeTableName()
                    .toLowerCase(), t);

            new_tlist.add(tuple);

            outstream.put(count++, new_tlist);

        }

        return outstream;
    }

    @SuppressWarnings("unchecked")
    public List<Tuple> calculateGroupByProjection() throws ParseException {
        List<Tuple> new_tlist = new ArrayList<Tuple>();
        StringBuilder sb = new StringBuilder();
        boolean isDistinct;
        selectList = select.getSelectItems();

        // COUNT/SUM for GROUPBY

        String key = getGroupByKey();

        //

        List<Tuple> tlist = instream.get(0);

        for (SelectExpressionItem s : selectList) {

            String col = new String();
            Expression exp = s.getExpression();
            ExpressionEvaluator ee = new ExpressionEvaluator();
            exp.accept(ee);

            if (ee.getUserColumn() != null) {
                col = ee.getUserColumn().getColumnName();

            } else {
                col = ee.getColumn().getWholeColumnName();

            }
            isDistinct = ee.getResult();

            if (col.contains("|")) {
                col = col.split("\\.", 2)[1];
                String fnct = col.split("\\|")[0];
                String expn = col.split("\\|")[1];
                if (fnct.equalsIgnoreCase("COUNT")) {

                    sb.append(new Count(instream, expn, isDistinct, key)
                            .getCount() + "|");

                } else if (fnct.equalsIgnoreCase("SUM")) {
                    sb.append(new Sum(instream, expn, isDistinct).getSum()
                            + "|");

                } else if (fnct.equalsIgnoreCase("AVG")) {
                    sb.append(new Avg(instream, expn, isDistinct).getAvg()
                            + "|");
                }

            } else {

                for (Tuple t : tlist) {

                    String val = t.getColValue(col);

                    if (val != null) {

                        sb.append(val + "|");
                        break;
                    }
                }
            }

        }
        String t = sb.substring(0, sb.length() - 1);
        Tuple tuple = new Tuple(OutputSchema.osTable.getWholeTableName()
                .toLowerCase(), t);

        new_tlist.add(tuple);
        return new_tlist;
    }

    public String createExpression(Tuple t, String condition,
            LinkedHashSet<String> colnames) {
        for (String s : colnames) {

            String val = t.getColValue(s);
            if (val != null) {
                if (checkString(val)) {
                    condition = condition.replace(s, "'" + val + "'");
                } else {

                    condition = condition.replace(s, val);
                }
            }

        }
        return condition;
    }

    boolean checkString(String s) {
        try {
            Integer.parseInt(s);
            return false;
        } catch (Exception e) {
            try {
                Double.parseDouble(s);
                return false;
            } catch (Exception e1) {
                return true;
            }
        }
    }

    public void calculate(Tuple t, String col) throws ParseException {

        Expression exp4 = Main.parseArithematicExpression(col);
        ExpressionEvaluator eee1 = new ExpressionEvaluator();
        exp4.accept(eee1);
        LinkedHashSet<String> colnames = eee1.getColNames();
        // System.out.println(colnames);
        String condition = createExpression(t, col, colnames);
        // System.out.println(condition);

        Expression exp5 = Main.parseArithematicExpression(condition);
        ExpressionEvaluator ee1 = new ExpressionEvaluator();
        exp5.accept(ee1);

        // l = ee1.getLongEvaluation();
        d = ee1.getDoulbeEvaluation();

    }

    public void calculate(Tuple t, String col, LinkedHashSet<String> colnames2)
            throws ParseException {

        String condition = createExpression(t, col, colnames2);
        // System.out.println(condition);
        //
        // Expression exp5 = Main.parseArithematicExpression(condition);
        // ExpressionEvaluator ee1 = new ExpressionEvaluator();
        // exp5.accept(ee1);

        // l = ee1.getLongEvaluation();

        d = new ArithematicEvaluator().calculate(condition);

    }

    //
    public List<Tuple> combineGroupByProjection() throws ParseException {
        List<Tuple> new_tlist = new ArrayList<Tuple>();
        StringBuilder sb = new StringBuilder();
        boolean isDistinct;
        selectList = select.getSelectItems();
        String key = getGroupByOSKey();
        List<Tuple> tlist = instream.get(0);

        for (SelectExpressionItem s : selectList) {

            String col = new String();
            Expression exp = s.getExpression();
            ExpressionEvaluator ee = new ExpressionEvaluator();
            exp.accept(ee);

            if (ee.getUserColumn() != null) {
                col = ee.getUserColumn().getColumnName();

            } else {
                col = ee.getColumn().getWholeColumnName();

            }
            isDistinct = ee.getResult();

            if (col.contains("|")) {
                col = col.split("\\.", 2)[1];
                String fnct = col.split("\\|")[0];
                String expn = col.split("\\|")[1];
                if (Main.ct.size() < 5) {
                    if (s.getAlias() != null) {
                        expn = s.getAlias();
                    }
                }
                if (fnct.equalsIgnoreCase("COUNT")) {

                    sb.append(new Count(instream, expn, isDistinct, key)
                            .combineCount() + "|");

                } else if (fnct.equalsIgnoreCase("SUM")) {

                    sb.append(new Sum(instream, expn, isDistinct).combineSum()
                            + "|");

                } else if (fnct.equalsIgnoreCase("AVG")) {
                    sb.append(new Avg(instream, expn, isDistinct).getAvg()
                            + "|");
                }

            } else {

                for (Tuple t : tlist) {
                    String val = t.getOsColValue(col);

                    if (val != null) {

                        sb.append(val + "|");
                        break;
                    }
                }
            }

        }
        String t = sb.substring(0, sb.length() - 1);
        Tuple tuple = new Tuple(OutputSchema.osTable.getWholeTableName()
                .toLowerCase(), t);

        new_tlist.add(tuple);
        return new_tlist;
    }

    private String getGroupByKey() {
        String key = "";
        for (Column c : EvaluateStatement.groupBy) {
            for (Tuple t : instream.get(0)) {
                String val = t.getColValue(c.getWholeColumnName());
                if (val != null) {
                    key += val;
                    break;
                }
            }

        }
        return key;
    }

    private String getGroupByOSKey() {
        String key = "";
        for (Column c : EvaluateStatement.groupBy) {
            for (Tuple t : instream.get(0)) {
                String val = t.getOsColValue(c.getWholeColumnName());
                if (val != null) {
                    key += val;
                    break;
                }
            }

        }
        return key;
    }
}
