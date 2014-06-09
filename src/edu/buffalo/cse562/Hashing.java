/**
 * 
 */

package edu.buffalo.cse562;

import net.sf.jsqlparser.schema.Table;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author Pratik
 */
public class Hashing {

    public static LinkedHashMap<Integer, List<Tuple>> outstream;
    public List<IndexWrapper> iw;
    public OperatorWhere wh;
    public static int key;

    public Hashing(List<IndexWrapper> iw, OperatorWhere wh) {
        outstream = new LinkedHashMap<Integer, List<Tuple>>();
        this.iw = iw;
        this.wh = wh;
        key = 0;
    }

    public void computeHash(List<Tuple> updated, int count) throws IOException {

        List<Tuple> tlist = new ArrayList<Tuple>();
        List<List<Tuple>> l_tlist;
        if (count > 1 || count == iw.size()) {
            // recursive calls

            if (count == iw.size()) {
                // reading file 1st call
                /*
                 * First Lookup - Read one line from biggest table and then
                 * lookup in second table according to expression
                 */
                String readTab = iw.get(0).getName();
                Table t = new Table();
                for (Table t1 : EvaluateStatement.tables) {
                    if (t1.getWholeTableName().equalsIgnoreCase(readTab)) {
                        t = t1;
                        break;
                    }
                }

                BufferedReader br = null;
                String s;
                br = new BufferedReader(new FileReader(Main.data_dir + "/"
                        + readTab + ".dat"));
                while ((s = br.readLine()) != null) {
                    count = iw.size();
                    Tuple tuple = new Tuple(t.getWholeTableName().toLowerCase(), s);
                    tlist.add(tuple);
                    l_tlist = lookup(tlist, iw.size() - count);
                    tlist = new ArrayList<Tuple>();
                    count--;
                    for (List<Tuple> l : l_tlist) {
                        computeHash(l, count);
                    }

                }
                br.close();
            } else {
                // rest of the tables
                l_tlist = lookup(updated, iw.size() - count);
                count--;
                for (List<Tuple> l : l_tlist) {
                    computeHash(l, count);
                }
            }

        } else {
            // calculate condition
            if (count == 0) {
                l_tlist = new ArrayList<List<Tuple>>();
                l_tlist.add(updated);
            } else {
                l_tlist = lookup(updated, iw.size() - count);
            }
            for (List<Tuple> l : l_tlist) {
                if (wh.calculateCondition(l)) {
                    outstream.put(key++, l);
                }
            }
        }

    }

    /**
     * @param tlist
     * @param count
     * @return
     */
    private List<List<Tuple>> lookup(List<Tuple> tlist, int count) {
        List<Tuple> list;
        List<Tuple> temp = tlist;
        List<List<Tuple>> out_list = new ArrayList<List<Tuple>>();
        if (tlist != null) {
            for (Tuple t : tlist) {
                String val = t.getColValue(iw.get(count).getFindValueOf());

                if (val != null) {
                    if (iw.get(count).getHashindex().containsKey(val)) {
                        list = iw.get(count).getHashindex().get(val);
                        for (Tuple t1 : list) {
                            temp.add(t1);
                            out_list.add(temp);
                            temp = tlist;
                        }
                    }
                    break;
                }
            }
        }

        return out_list;
    }

}
