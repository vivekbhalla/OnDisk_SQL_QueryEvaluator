
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;

import java.text.SimpleDateFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OperatorWhere {
    private final Expression where;
    private List<Tuple> instream;
    private final LinkedHashSet<String> colnames;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public OperatorWhere(LinkedHashSet<String> colnames, Expression where) {
        // TODO Auto-generated constructor stub
        this.where = where;
        this.colnames = colnames;
    }

    public boolean calculateCondition(List<Tuple> instream) {

        try {
            if (where != null) {
                // instream.get(0).printTuple();
                String condition = createExpression(instream);
                // System.out.println(condition);
                Expression exp2 = Main.parseGeneralExpression(condition);
                ExpressionEvaluator e2 = new ExpressionEvaluator();
                exp2.accept(e2);
                // System.out.println(e2.getResult());
                return e2.getResult();
            } else {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /*
     * Function to replace values from tuple in the expression. For example:
     * Tuple: ABDELAL01|Alaa|Abdelnaby|1990|1994|240|1968-06-24 Expression:
     * FIRSTSEASON >= 1980 AND FIRSTSEASON < 1991 result: 1990 >= 1980 AND 1990
     * < 1991
     */

    public String createExpression(List<Tuple> instream) {

        String condition = where.toString();

        for (String s : colnames) {

            for (Tuple t : instream) {
                String val = t.getColValue(s);

                if (val != null) {
                    if (checkDate(val)) {
                        condition = condition.replaceAll("\\bDATE\\b\\('", "{d'");
                        condition = condition.replaceAll("\\bdate\\b\\('", "{d'");
                        Pattern regex = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}')(\\))");
                        Matcher regexMatcher = regex.matcher(condition);
                        condition = regexMatcher.replaceAll("$1\\}"); // *3 ??
                        String date2 = "{d'" + val + "'}";
                        condition = condition.replace(s, date2);
                    } else if (checkString(val)) {
                        condition = condition.replace(s, "'" + val + "'");
                    } else {
                        condition = condition.replace(s, val);
                    }
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

    boolean checkDate(String s) {
        try {
            sdf.parse(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
