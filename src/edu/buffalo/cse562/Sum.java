
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;

import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

public class Sum {
    private final LinkedHashMap<Integer, List<Tuple>> instream;
    private String exp;
    private LinkedHashSet<String> colList;
    @SuppressWarnings("unused")
    private final boolean isDistinct;

    public Sum(LinkedHashMap<Integer, List<Tuple>> instream, String exp,
            boolean isDistinct) {
        this.instream = instream;
        this.exp = exp;
        colList = new LinkedHashSet<String>();
        this.isDistinct = isDistinct;
    }

    public String getSum() throws ParseException {

        Expression exp2 = Main.parseArithematicExpression(exp);
        ExpressionEvaluator e2 = new ExpressionEvaluator();
        exp2.accept(e2);
        colList = e2.getColNames();

        Double sum = new Double(0);
        for (List<Tuple> l : instream.values()) {
            String condition = createExpression(l);
            Expression exp3 = Main.parseArithematicExpression(condition);
            ExpressionEvaluator e3 = new ExpressionEvaluator();
            exp3.accept(e3);
            sum = sum + e3.getDoulbeEvaluation();
        }

        DecimalFormat f = new DecimalFormat("##0.0##");
        return f.format(sum).toString();
    }

    public String createExpression(List<Tuple> instream) {

        String condition = exp;
        for (String s : colList) {
            for (Tuple t : instream) {
                String val = t.getColValue(s);
                if (val != null) {
                    Double d = Double.parseDouble(val);
                    condition = condition.replace(s, d.toString());
                }
            }
        }
        return condition;
    }

    public String combineSum() {
        double sum = 0;
        exp = exp.replaceAll("\\(", "");
        exp = exp.replaceAll("\\)", "");

        if (instream != null) {
            for (Integer i : instream.keySet()) {

                for (Tuple t : instream.get(i)) {
                    String val = t.getOsColValue(exp);

                    if (val != null) {
                        sum = sum + Double.parseDouble(val);
                        break;
                    }
                }
            }
            DecimalFormat f = new DecimalFormat("##0.0##");
            return f.format(sum).toString();
        } else {
            return null;
        }

    }

}
