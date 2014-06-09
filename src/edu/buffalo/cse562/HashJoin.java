/**
 * 
 */

package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * @author Pratik
 */
public class HashJoin {

    public List<HashSet<Tuple>> and_join(List<Table> tables,
            List<Expression> where_exp) throws IOException {

        for (Expression w : where_exp) {
            List<Expression> or_exp = null;
            ExpressionEvaluator e = new ExpressionEvaluator();
            Expression exp = w;
            exp.accept(e);
            or_exp = e.or_getExpList();
            if (or_exp.size() >= 1) {

                or_join(or_exp, tables);
            } else {
                LinkedHashSet<String> colnames;
                List<String> tab_list = new ArrayList<String>();
                List<String> col_list = new ArrayList<String>();
                ExpressionEvaluator e1 = new ExpressionEvaluator();
                w.accept(e1);
                colnames = e1.getColNames();
                for (String tab : colnames) {
                    if (tab.contains(".")) {
                        tab_list.add(tab.split("\\.")[0]);
                        col_list.add(tab.split("\\.")[1]);
                    }
                }

                String tab1 = tab_list.get(0);
                if (tab_list.size() > 1) {

                    String tab2 = tab_list.get(1);

                    String col1 = col_list.get(0);
                    String col2 = col_list.get(1);

                    long size1, size2;

                    /*
                     * Checking the file size, to decide which table will be
                     * included in the Build Phase and which one in the Probe
                     * Phase
                     */
                    File f1 = new File(Main.swap_dir + "/" + tab1 + ".dat");
                    if (f1.exists() && !f1.isDirectory()) {
                        size1 = f1.length();
                    } else {
                        f1 = new File(Main.data_dir + "/" + tab1 + ".dat");
                        size1 = f1.length();

                    }

                    File f2 = new File(Main.swap_dir + "/" + tab2 + ".dat");
                    if (f2.exists() && !f2.isDirectory()) {
                        size2 = f2.length();
                    } else {
                        f2 = new File(Main.data_dir + "/" + tab2 + ".dat");
                        size2 = f2.length();
                    }
                    /* File size checking Ends */

                    /*
                     * Set the lookup table and outer for loop table for the
                     * Hash Join
                     */
                    HashMap<String, List<Tuple>> lookup = null;
                    File f = null;
                    String col = null;
                    Table tab = null;
                    if (size1 < size2) {

                        for (Table t : tables) {
                            if (t.getWholeTableName().equalsIgnoreCase(tab1)) {
                                lookup = createHashTable(f1, t, col1);
                                col = col2;

                                break;
                            }
                        }

                        for (Table t : tables) {
                            if (t.getWholeTableName().equalsIgnoreCase(tab2)) {
                                tab = t;
                                break;
                            }
                        }

                        f = f2;
                    } else {
                        for (Table t : tables) {
                            if (t.getWholeTableName().equalsIgnoreCase(tab2)) {
                                lookup = createHashTable(f2, t, col2);
                                col = col1;

                                break;
                            }
                        }

                        for (Table t : tables) {
                            if (t.getWholeTableName().equalsIgnoreCase(tab1)) {
                                tab = t;
                                break;
                            }
                        }

                        f = f1;
                    }

                    // Hash Join Implementation
                    HashSet<Tuple> out_lookup = new HashSet<Tuple>();
                    List<Tuple> temp_lookup = new ArrayList<Tuple>();
                    File temp = new File(Main.swap_dir + "/" + "temp.dat");
                    temp.createNewFile();

                    PrintWriter pw = new PrintWriter(new FileWriter(
                            temp.getAbsolutePath()));

                    BufferedReader br = null;

                    String s;
                    br = new BufferedReader(new FileReader(f.getAbsolutePath()));
                    while ((s = br.readLine()) != null) {

                        Tuple tuple = new Tuple(tab.getWholeTableName().toLowerCase(), s);

                        if (lookup.containsKey(tuple.getColValue(col))) {

                            temp_lookup = lookup.get(tuple.getColValue(col));
                            for (Tuple t : temp_lookup) {
                                out_lookup.add(t);

                            }
                            pw.println(s);
                        }
                    }
                    br.close();
                    pw.close();

                    File checkfile = new File(Main.swap_dir + "/"
                            + f.getName());

                    if (checkfile.exists() && !checkfile.isDirectory()) {

                        checkfile.delete();
                        temp.renameTo(new File(Main.swap_dir + "/"
                                + f.getName()));
                    } else {
                        temp.renameTo(new File(Main.swap_dir + "/"
                                + f.getName()));
                    }

                    String tab_lookup = tab1;
                    if (tab.getWholeTableName().equals(tab1)) {
                        tab_lookup = tab2;
                    }

                    PrintWriter pw2 = new PrintWriter(new FileWriter(
                            Main.swap_dir + "/" + tab_lookup + ".dat"), false);

                    for (Tuple t : out_lookup) {

                        pw2.println(t.toString());
                    }

                    pw2.close();

                } else {
                    // single table
                    Table tab = new Table();
                    for (Table t : tables) {
                        if (t.getWholeTableName().equalsIgnoreCase(tab1)) {
                            tab = t;
                            break;
                        }
                    }

                    File f1 = new File(Main.swap_dir + "/" + tab1 + ".dat");
                    if (!f1.exists()) {
                        f1 = new File(Main.data_dir + "/" + tab1 + ".dat");
                    }

                    OperatorWhere wh = new OperatorWhere(colnames, w);
                    File temp = new File(Main.swap_dir + "/" + "temp.dat");
                    temp.createNewFile();

                    PrintWriter pw = new PrintWriter(new FileWriter(
                            temp.getAbsolutePath()));

                    BufferedReader br = null;

                    String s;
                    br = new BufferedReader(
                            new FileReader(f1.getAbsolutePath()));
                    while ((s = br.readLine()) != null) {

                        Tuple tuple = new Tuple(tab.getWholeTableName().toLowerCase(), s);

                        List<Tuple> tlist = new ArrayList<Tuple>();
                        tlist.add(tuple);
                        if (wh.calculateCondition(tlist)) {
                            pw.println(s);
                        }
                    }
                    br.close();
                    pw.close();
                }
            }
        }

        List<HashSet<Tuple>> tablesInMemory = new ArrayList<HashSet<Tuple>>();
        for (Table t : tables) {
            String dat_file = Main.swap_dir + "/" + t.getWholeTableName()
                    + ".dat";
            File f = new File(dat_file);
            if (f.exists() && !f.isDirectory()) {

                tablesInMemory.add(new MultipleTable(null, null).readTable(
                        dat_file, t));
            }
        }

        return tablesInMemory;
    }

    /**
     * @param f1
     * @return
     */
    private HashMap<String, List<Tuple>> createHashTable(File f1, Table t,
            String col) {
        HashMap<String, List<Tuple>> lookup = new HashMap<String, List<Tuple>>();
        BufferedReader br = null;
        try {
            String s;
            br = new BufferedReader(new FileReader(f1.getAbsolutePath()));
            while ((s = br.readLine()) != null) {

                Tuple tuple = new Tuple(t.getWholeTableName().toLowerCase(), s);

                if (lookup.containsKey(tuple.getColValue(col))) {
                    lookup.get(tuple.getColValue(col)).add(tuple);
                } else {
                    List<Tuple> tlist = new ArrayList<Tuple>();
                    tlist.add(tuple);
                    lookup.put(tuple.getColValue(col), tlist);
                }

            }

            return lookup;

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        }

        return null;
    }

    /**
     * @param or_exp
     * @param tables
     */
    private void or_join(List<Expression> or_exp, List<Table> tables) {

        for (Expression w : or_exp) {

            LinkedHashSet<String> colnames;
            List<String> tab_list = new ArrayList<String>();
            ExpressionEvaluator e1 = new ExpressionEvaluator();
            w.accept(e1);
            colnames = e1.getColNames();
            for (String tab : colnames) {
                tab_list.add(tab.split("\\.")[0]);
            }

        }

    }
}
