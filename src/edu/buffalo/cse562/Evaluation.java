
package edu.buffalo.cse562;

import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Evaluation {
    public static Queue<PlainSelect> plain_selects = new LinkedList<PlainSelect>();
    private LinkedHashMap<Integer, List<Tuple>> outstream;

    /**
     * Print the final output to STDOUT
     * 
     * @throws ParseException
     */
    public void evaluate_sql() throws ParseException {

        for (Select s : Main.select) {

            SelectEvaluator e = new SelectEvaluator();
            s.getSelectBody().accept(e);
            if (plain_selects.size() == 1) {
                /* Process the query */
                try {
                    outstream = new EvaluateStatement(s).processQuery();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }

                PlainSelect p = (PlainSelect) s.getSelectBody();
                /* Print the Final Output */
                long l = outstream.keySet().size();
                if (p.getLimit() != null) {
                    l = p.getLimit().getRowCount();
                }
                for (Integer i : outstream.keySet()) {
                    if (i == l) {
                        break;
                    }
                    for (Tuple t1 : outstream.get(i)) {
                        String s1[] = t1.getTupleVaule();
                        for (int c = 0; c < s1.length - 1; c++) {
                            System.out.print(s1[c] + "|");
                        }
                        System.out.print(s1[s1.length - 1]);
                    }
                    System.out.println();
                }

            } else {
                /* To implement union between different selects */
            }

            /* Reset the select list, if another query is present. */
            plain_selects = new LinkedList<PlainSelect>();
        }
    }
}
