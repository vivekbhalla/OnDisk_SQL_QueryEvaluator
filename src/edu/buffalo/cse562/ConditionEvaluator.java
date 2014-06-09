/**
 * 
 */

package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Pratik
 */

public class ConditionEvaluator {

    private final List<IndexWrapper> ilist;
    public static int BINS;
    static StringBuilder[] sb_array;
    static int linecount;
    static int limit;
    private final SimpleDateFormat sdf;
    private final List<Expression> where_exp;
    private static HashMap<String, TableExpInfo> tabinfo;
    private final List<int[]> offset;
    private final List<Table> finalTabList;
    ConditionExpressionEvaluator E;
    Expression EXP;
    Select select;
    private final HashMap<String, List<Integer>> projectOffsets;
    public static List<CreateTable> newctList;
    String finaltab;

    ConditionEvaluator(List<IndexWrapper> ilist, List<Expression> where_exp,
            Select select) {
        newctList = new ArrayList<CreateTable>();
        projectOffsets = new HashMap<String, List<Integer>>();
        this.select = select;
        this.ilist = ilist;
        BINS = 25;
        sb_array = initTmap();
        linecount = 0;
        limit = 2000000;
        tabinfo = new HashMap<String, TableExpInfo>();
        sdf = new SimpleDateFormat("yyyy-MM-dd");
        this.where_exp = where_exp;

        finalTabList = new ArrayList<Table>();
        offset = new ArrayList<int[]>();

        E = new ConditionExpressionEvaluator();
    }

    public void binHash() {
        getSingleExpressions();
        // printtabinfo();
        int count = 0;
        HashMap<Integer, List<String>> inMem = new HashMap<Integer, List<String>>();
        boolean makeBins = true;
        String next[];

        for (IndexWrapper i : ilist) {
            makeBins = true;
            // checking smaller table to create in memory hash map
            if (count == ilist.size() - 1) {
                outstreamCreator(i.getTables(), i.getColumns(), count);
                Main.ct = newctList;

            } else {
                if (count == 0) {
                    if (i.getSize()[0] < i.getSize()[1]) {
                        if (i.getSize()[0] < 100) {
                            inMem = getInMemoryMap(i.getTables().get(0),
                                    i.getColumns()[0], true);
                            String newtab = i.getTables().get(1) + "+"
                                    + i.getTables().get(0);
                            next = getNextKey(count, i.getTables(), newtab);
                            readlineNlookup(inMem, i.getTables().get(1),
                                    i.getColumns()[1], next[0], next[1], newtab);
                            makeBins = false;
                        }

                    } else {
                        if (i.getSize()[1] < 100) {
                            inMem = getInMemoryMap(i.getTables().get(1),
                                    i.getColumns()[1], true);

                            String newtab = i.getTables().get(0) + "+"
                                    + i.getTables().get(1);

                            next = getNextKey(count, i.getTables(), newtab);
                            readlineNlookup(inMem, i.getTables().get(0),
                                    i.getColumns()[0], next[0], next[1], newtab);

                            makeBins = false;
                        }
                    }
                }
                // making bins
                if (makeBins) {

                    bincreatorNjoin(i.getTables(), i.getColumns(), count);

                }
                count++; // ilist count
            }
        }
    }

    /**
     * @param tables
     * @param columns
     * @param count
     */
    private void bincreatorNjoin(List<String> tables, String[] columns,
            int count) {
        // TODO Auto-generated method stub
        String breakBinsTab = tables.get(0);
        String breakBinsCol = columns[0];

        String joinTab = tables.get(1);
        String joinCol = columns[1];
        if (tables.get(0).contains("+")) {
            breakBinsTab = tables.get(1);
            breakBinsCol = columns[1];
            joinTab = tables.get(0);
            joinCol = columns[0];
        }

        String newtab = breakBinsTab + "+" + joinTab;
        String next[] = getNextKey(count, ilist.get(count).getTables(), newtab);
        createBins(breakBinsTab, breakBinsCol);

        // long endTime = System.currentTimeMillis();
        // long totalTime = endTime - Main.startTime;
        // System.out.println("Time After create bin: " + totalTime / 1000
        // + " seconds");

        joinBins(joinTab, joinCol, breakBinsTab, breakBinsCol, next[0],
                next[1], newtab);

        // endTime = System.currentTimeMillis();
        // totalTime = endTime - Main.startTime;
        // System.out.println("Time After Join: " + totalTime / 1000 +
        // " seconds");

    }

    private void outstreamCreator(List<String> tables, String[] columns,
            int count) {
        // TODO Auto-generated method stub
        String breakBinsTab = tables.get(0);
        String breakBinsCol = columns[0];

        String joinTab = tables.get(1);
        String joinCol = columns[1];
        if (tables.get(0).contains("+")) {
            breakBinsTab = tables.get(1);
            breakBinsCol = columns[1];
            joinTab = tables.get(0);
            joinCol = columns[0];
        }

        String newtab = breakBinsTab + "+" + joinTab;
        finaltab = newtab;
        createBins(breakBinsTab, breakBinsCol);

        if (count == 0) {
            createBins(joinTab, joinCol);
        }

        // long endTime = System.currentTimeMillis();
        // long totalTime = endTime - Main.startTime;
        // System.out.println("Time After lineitem create bin: " + totalTime
        // / 1000 + " seconds");

        createOffsets(newtab);

        joinBins(joinTab, joinCol, breakBinsTab, breakBinsCol, null, null,
                newtab);

        // endTime = System.currentTimeMillis();
        // totalTime = endTime - Main.startTime;
        // System.out.println("Time After lineitem Join: " + totalTime / 1000
        // + " seconds");

    }

    private void createOffsets(String newtab) {
        // TODO Auto-generated method stub
        int count = 0;
        for (String s : newtab.split("\\+")) {

            for (CreateTable t : Main.ct) {
                if (s.equalsIgnoreCase(t.getTable().getWholeTableName())) {
                    CreateTable newct = new CreateTable();

                    int[] temp = {
                            count,
                            count + t.getColumnDefinitions().size()
                    };
                    offset.add(temp);
                    count += t.getColumnDefinitions().size();
                    PushProjection p = new PushProjection(select);
                    newct.setColumnDefinitions(p.coldefs(t));
                    projectOffsets.put(t.getTable().getWholeTableName()
                            .toLowerCase(), p.getProjectOffset());
                    Table tab = t.getTable();
                    newct.setTable(tab);
                    finalTabList.add(tab);
                    newctList.add(newct);
                    break;
                }

            }
        }
    }

    /**
     * @param joinTab
     * @param breakBinsTab
     * @param breakBinsCol
     * @param newtab
     * @param next2
     * @param next
     */
    private void joinBins(String joinTab, String joinCol, String breakBinsTab,
            String breakBinsCol, String next1, String next2, String newtab) {
        // TODO Auto-generated method stub
        HashMap<Integer, List<String>> inMemBin = new HashMap<Integer, List<String>>();

        for (int i = 0; i < BINS; i++) {
            inMemBin = getInMemoryMap(joinTab + "-" + i, joinCol, false);
            if (next1 != null) {
                readlineNlookup(inMemBin, breakBinsTab + "-" + i, breakBinsCol,
                        next1, next2, newtab);
            } else {

                readlineNlookup(inMemBin, breakBinsTab + "-" + i, breakBinsCol,
                        newtab);

            }

        }
    }

    private void createBins(String tab, String col) {
        BufferedReader br = null;
        int key;
        int lines = 0;
        String s;
        StringBuilder sb_array[] = initTmap();
        int cnt = getColNumber(col, tab);

        ArrayList<Metadata>[] mlist = null;
        String expression = null;
        String[] colNames = null;
        int[] cntList = null;
        boolean isConditionPresent = false;
        String[] datatype = null;
        BooleanEvaluator be = null;
        if (tabinfo.containsKey(tab)) {
            colNames = new String[tabinfo.get(tab).getCol().size()];
            datatype = new String[tabinfo.get(tab).getCol().size()];
            tabinfo.get(tab).setCntList();
            tabinfo.get(tab).setDatatype();
            expression = tabinfo.get(tab).getExp();
            int count = 0;
            for (String temp : tabinfo.get(tab).getCol()) {
                colNames[count++] = temp;
            }
            cntList = tabinfo.get(tab).getCntList();
            datatype = tabinfo.get(tab).getDatatype();

            mlist = tabinfo.get(tab).getAndOrList();
            be = new BooleanEvaluator();
            isConditionPresent = true;
        }
        File f = new File(Main.data_dir + "/" + tab + ".dat");
        if (!f.exists()) {
            f = new File(Main.swap_dir + "/" + tab + ".dat");
        }

        try {
            br = new BufferedReader(new FileReader(f));

            while ((s = br.readLine()) != null) {
                if (isConditionPresent) {
                    // if (calculateCondition(s, tab, expression, colNames,
                    // cntList, datatype)) {
                    if (be.calculateBoolean(mlist, s)) {
                        lines++;
                        key = Integer.parseInt(s.split("\\|")[cnt]) % BINS;
                        sb_array[key].append(s + "\n");
                        if (lines > limit) {
                            flushToDisk(sb_array, tab);
                            sb_array = initTmap();
                            // System.gc();
                            lines = 0;
                        }
                    }
                } else {
                    lines++;
                    key = Integer.parseInt(s.split("\\|")[cnt]) % BINS;
                    sb_array[key].append(s + "\n");
                    if (lines > limit) {
                        flushToDisk(sb_array, tab);
                        sb_array = initTmap();
                        // System.gc();
                        lines = 0;
                    }
                }
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        flushToDisk(sb_array, tab);
        sb_array = initTmap();
        // System.gc();
    }

    /**
     * @param count
     * @param currlist
     * @param newtab
     * @return
     */
    private String[] getNextKey(int count, List<String> currlist, String newtab) {
        // TODO Auto-generated method stub
        List<String> ntablist = new ArrayList<String>();

        String[] temp = new String[2];

        String next[] = new String[2];
        for (String cur : currlist) {
            int num = 0;
            for (String nxt : ilist.get(count + 1).getTables()) {
                if (nxt.equalsIgnoreCase(cur)) {
                    next[0] = nxt;
                    next[1] = ilist.get(count + 1).getColumns()[num];
                    // modifying next entry in ilist
                    temp[num] = newtab;
                    temp[num == 0 ? 1 : 0] = ilist.get(count + 1).getTables()
                            .get(num == 0 ? 1 : 0);
                    ntablist.add(temp[0]);
                    ntablist.add(temp[1]);
                    ilist.get(count + 1).setTables(ntablist);
                    break;
                }
                num++;

            }
        }
        return next;
    }

    /**
     * @param inMemBin
     * @param string
     * @param breakBinsCol
     * @param newtab
     */
    private void readlineNlookup(HashMap<Integer, List<String>> inMem,
            String currtab, String currcol, String newtab) {
        // TODO Auto-generated method stub
        String currtab1 = currtab;
        if (currtab.contains("-")) {
            currtab1 = currtab1.split("-")[0];
        }
        // TODO Auto-generated method stub
        // boolean iscurrtab = false;
        int cnt = getColNumber(currcol, currtab1);
        BufferedReader br = null;
        int key;
        String s;
        StringBuilder sb = new StringBuilder();
        File f = new File(Main.data_dir + "/" + currtab + ".dat");
        if (!f.exists()) {
            f = new File(Main.swap_dir + "/" + currtab + ".dat");
        }

        try {
            br = new BufferedReader(new FileReader(f));

            while ((s = br.readLine()) != null) {

                key = Integer.parseInt(s.split("\\|")[cnt]);
                if (inMem.containsKey(key)) {
                    for (String t : inMem.get(key)) {
                        sb.append(s + "|").append(t);
                        // sb.append(s).append(t);
                        // CALL OUR METHOD HERE
                        addToOutstream(sb.toString(), key);
                        sb = new StringBuilder();
                        // sb_array[key % BINS].append(s).append(t + "\n");
                        // sb_array[key % BINS].append(s + "|").append(t +
                        // "\n");
                        linecount++;
                    }

                } // if containsKey ends
                if (linecount > limit) {
                    flushToDisk(sb_array, newtab);
                    sb_array = initTmap();
                    System.gc();
                    linecount = 0;
                }
            } // while loop ends

            // Write to disk?
            flushToDisk(sb_array, newtab);
            linecount = 0;
            inMem.clear();
            System.gc();
            sb_array = initTmap();
            br.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void addToOutstream(String record, int key) {
        key = key % BINS;
        int count = 0;
        String[] temp1;
        List<Integer> off;
        for (int[] o : offset) {
            off = projectOffsets.get(finalTabList.get(count++)
                    .getWholeTableName().toLowerCase());
            temp1 = Arrays.copyOfRange(record.split("\\|"), o[0], o[1]);
            for (int i : off) {
                sb_array[key].append(temp1[i] + "|");
            }
        }
        sb_array[key].append("\n");
    }

    // public LinkedHashMap<Integer, List<Tuple>> addToOutstream(int binno) {
    //
    // String s;
    // File f = new File(Main.swap_dir + "/" + finaltab + "-" + binno + ".dat");
    // LinkedHashMap<Integer, List<Tuple>> Outstream = new
    // LinkedHashMap<Integer, List<Tuple>>();
    // int OC = 0;
    //
    // BufferedReader br = null;
    // try {
    // br = new BufferedReader(new FileReader(f));
    // while ((s = br.readLine()) != null) {
    // String[] temp1, temp2;
    // List<Integer> off;
    // Table t;
    // List<Tuple> tlist = new ArrayList<Tuple>();
    // int count = 0;
    //
    // for (int[] o : offset) {
    // t = finalTabList.get(count++);
    // off = projectOffsets.get(t.getWholeTableName()
    // .toLowerCase());
    //
    // if (off.size() > 0) {
    // temp1 = Arrays.copyOfRange(s.split("\\|"), o[0], o[1]);
    // int num = 0;
    // temp2 = new String[off.size()];
    // for (int i : off) {
    // temp2[num++] = temp1[i];
    // }
    // Tuple tup = new Tuple(t, temp2);
    // tlist.add(tup);
    // }
    // }
    // Outstream.put(OC++, tlist);
    //
    // } // while ends
    // br.close();
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    //
    // return Outstream;
    // }

    public LinkedHashMap<Integer, List<Tuple>> addToOutstream(int binno) {

        String s;
        File f = new File(Main.swap_dir + "/" + finaltab + "-" + binno + ".dat");
        LinkedHashMap<Integer, List<Tuple>> Outstream = new LinkedHashMap<Integer, List<Tuple>>();
        int OC = 0;
        // printtabinfo();
        // System.out.println(finalTabList);
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(f));
            while ((s = br.readLine()) != null) {

                List<Tuple> tlist = new ArrayList<Tuple>();
                int start = 0;
                int end = 0;
                String temp[];
                List<Integer> off;
                for (Table t : finalTabList) {

                    off = projectOffsets.get(t.getWholeTableName()
                            .toLowerCase());
                    if (off.size() > 0) {
                        end += off.size();
                        temp = Arrays.copyOfRange(s.split("\\|"), start, end);
                        Tuple tup = new Tuple(t.getWholeTableName()
                                .toLowerCase(), temp);
                        // tup.printTuple();
                        tlist.add(tup);
                        start = end;
                    }
                }
                Outstream.put(OC++, tlist);

            } // while ends
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Outstream;
    }

    /**
     * @param inMem
     * @param newtab
     * @param string
     * @param string2
     * @param count
     */
    private void readlineNlookup(HashMap<Integer, List<String>> inMem,
            String currtab, String currcol, String nxttab, String nxtcol,
            String newtab) {
        String currtab1 = currtab;
        if (currtab.contains("-")) {
            currtab1 = currtab1.split("-")[0];
        }
        // TODO Auto-generated method stub
        boolean iscurrtab = false;
        int cnt = getColNumber(currcol, currtab1);
        int nxtcnt = getColNumber(nxtcol, nxttab);

        if (currtab1.equalsIgnoreCase(nxttab)) {
            iscurrtab = true;
        }
        BufferedReader br = null;
        int key, nxtkey;
        String s;
        File f = new File(Main.data_dir + "/" + currtab + ".dat");
        if (!f.exists()) {
            f = new File(Main.swap_dir + "/" + currtab + ".dat");
        }

        String expression = null;
        String[] colNames = null;
        int[] cntList = null;
        boolean isConditionPresent = false;
        String[] datatype = null;
        ArrayList<Metadata>[] mlist = null;
        BooleanEvaluator be = null;
        if (tabinfo.containsKey(currtab1)) {
            colNames = new String[tabinfo.get(currtab1).getCol().size()];
            datatype = new String[tabinfo.get(currtab1).getCol().size()];
            tabinfo.get(currtab1).setCntList();
            tabinfo.get(currtab1).setDatatype();
            expression = tabinfo.get(currtab1).getExp();
            int count = 0;
            for (String temp : tabinfo.get(currtab1).getCol()) {
                colNames[count++] = temp;
            }
            cntList = tabinfo.get(currtab1).getCntList();
            datatype = tabinfo.get(currtab1).getDatatype();

            mlist = tabinfo.get(currtab1).getAndOrList();
            be = new BooleanEvaluator();

            isConditionPresent = true;
        }

        try {
            br = new BufferedReader(new FileReader(f));

            while ((s = br.readLine()) != null) {
                if (isConditionPresent) {
                    // if (calculateCondition(s, currtab1, expression, colNames,
                    // cntList, datatype)) {
                    if (be.calculateBoolean(mlist, s)) {
                        key = Integer.parseInt(s.split("\\|")[cnt]);
                        if (inMem.containsKey(key)) {
                            if (iscurrtab) {
                                nxtkey = Integer
                                        .parseInt(s.split("\\|")[nxtcnt])
                                        % BINS;
                                for (String t : inMem.get(key)) {
                                    // sb_array[nxtkey].append(s).append(t +
                                    // "\n");
                                    sb_array[nxtkey].append(s + "|").append(
                                            t + "\n");
                                    linecount++;
                                }
                            } else {
                                for (String t : inMem.get(key)) {
                                    nxtkey = Integer
                                            .parseInt(t.split("\\|")[nxtcnt])
                                            % BINS;
                                    // sb_array[nxtkey].append(s).append(t +
                                    // "\n");
                                    sb_array[nxtkey].append(s + "|").append(
                                            t + "\n");
                                    linecount++;
                                }

                            } // if else currtab ends
                        } // if containsKey ends
                    }
                } else {
                    key = Integer.parseInt(s.split("\\|")[cnt]);
                    if (inMem.containsKey(key)) {
                        if (iscurrtab) {
                            nxtkey = Integer.parseInt(s.split("\\|")[nxtcnt])
                                    % BINS;
                            for (String t : inMem.get(key)) {
                                // sb_array[nxtkey].append(s).append(t + "\n");
                                sb_array[nxtkey].append(s + "|").append(
                                        t + "\n");
                                linecount++;
                            }
                        } else {
                            for (String t : inMem.get(key)) {
                                nxtkey = Integer
                                        .parseInt(t.split("\\|")[nxtcnt])
                                        % BINS;
                                // sb_array[nxtkey].append(s).append(t + "\n");
                                sb_array[nxtkey].append(s + "|").append(
                                        t + "\n");
                                linecount++;
                            }

                        } // if else currtab ends
                    } // if containsKey ends

                }
                if (linecount > limit / currtab.split("\\+").length + 1) {
                    flushToDisk(sb_array, newtab);
                    sb_array = initTmap();
                    // System.gc();
                    linecount = 0;
                }
            } // while loop ends

            // Write to disk?
            flushToDisk(sb_array, newtab);
            linecount = 0;
            inMem.clear();
            // System.gc();
            sb_array = initTmap();

            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void flushToDisk(StringBuilder[] sb_array, String newtab) {

        try {
            for (int i = 0; i < sb_array.length; i++) {
                PrintWriter pw = new PrintWriter(new FileWriter(Main.swap_dir
                        + "/" + newtab + "-" + i + ".dat", true));

                pw.write(sb_array[i].toString());
                pw.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public StringBuilder[] initTmap() {
        StringBuilder[] sb = new StringBuilder[BINS];
        for (int i = 0; i < BINS; i++) {
            sb[i] = new StringBuilder();
        }
        return sb;
    }

    /**
     * @param col
     * @param string
     * @return
     */
    private HashMap<Integer, List<String>> getInMemoryMap(String tab,
            String col, boolean isSmall) {
        // TODO Auto-generated method stub
        HashMap<Integer, List<String>> inMem = new HashMap<Integer, List<String>>();

        String tableName = tab;
        if (tab.contains("-")) {
            tableName = tab.split("-")[0];
        }
        int cnt = getColNumber(col, tableName);
        BufferedReader br = null;
        int key;
        String s;
        File f = new File(Main.data_dir + "/" + tab + ".dat");
        if (!f.exists()) {
            f = new File(Main.swap_dir + "/" + tab + ".dat");
        }

        String expression = null;
        String[] colNames = null;
        int[] cntList = null;
        boolean isConditionPresent = false;

        String[] datatype = null;
        BooleanEvaluator be = null;
        ArrayList<Metadata>[] mlist = null;
        if (tabinfo.containsKey(tableName)) {
            colNames = new String[tabinfo.get(tableName).getCol().size()];
            datatype = new String[tabinfo.get(tableName).getCol().size()];
            tabinfo.get(tableName).setCntList();
            tabinfo.get(tableName).setDatatype();
            expression = tabinfo.get(tableName).getExp();
            int count = 0;
            for (String temp : tabinfo.get(tableName).getCol()) {
                colNames[count++] = temp;
            }
            cntList = tabinfo.get(tableName).getCntList();
            datatype = tabinfo.get(tableName).getDatatype();

            mlist = tabinfo.get(tableName).getAndOrList();
            be = new BooleanEvaluator();
            isConditionPresent = true;
        }

        try {
            br = new BufferedReader(new FileReader(f));

            while ((s = br.readLine()) != null) {
                if (isConditionPresent) {
                    // if (calculateCondition(s, tableName, expression,
                    // colNames,
                    // cntList, datatype)) {
                    if (be.calculateBoolean(mlist, s)) {
                        key = Integer.parseInt(s.split("\\|")[cnt]);
                        if (inMem.containsKey(key)) {
                            inMem.get(key).add(s);
                        } else {
                            List<String> slist = new ArrayList<String>();
                            slist.add(s);
                            inMem.put(key, slist);
                        }
                    }
                } else {
                    key = Integer.parseInt(s.split("\\|")[cnt]);
                    if (inMem.containsKey(key)) {
                        inMem.get(key).add(s);
                    } else {
                        List<String> slist = new ArrayList<String>();
                        slist.add(s);
                        inMem.put(key, slist);
                    }
                } // isSmall if-else ends
            } // while ends
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return inMem;
    }

    public static int getColNumber(String col, String maintable) {

        String table = maintable;
        if (col.contains(".")) {
            table = col.split("\\.")[0];
            col = col.split("\\.")[1];
        }

        int count = 0;
        for (CreateTable ct : Main.ct) {

            String t1 = ct.getTable().toString();

            if (t1.equalsIgnoreCase(table)) {

                for (Object s : ct.getColumnDefinitions()) {
                    String col_defn = s.toString();

                    if (col_defn.contains(col)) {

                        break;
                    }

                    count++;
                }
                if (maintable.contains("+")) {
                    int newcount = count;
                    for (String s : maintable.split("\\+")) {
                        if (table.equalsIgnoreCase(s)) {
                            return newcount;
                        } else {
                            for (CreateTable ct1 : Main.ct) {
                                String t2 = ct1.getTable().toString();
                                if (t2.equalsIgnoreCase(s)) {
                                    newcount = ct1.getColumnDefinitions()
                                            .size() + newcount;
                                }
                            }
                        }

                    }

                } else {

                    return count;
                }
            }
        }
        return 0;
    }

    public boolean calculateCondition(String record, String tabname,
            String exp, String[] colnames, int[] cntList, String[] datatype) {
        try {
            // String condition = createExpression(record, tabname, exp,
            // colnames);
            // Expression exp2 = Main.parseGeneralExpression(condition);
            EXP = Main.parseGeneralExpression(createExpression(record, tabname,
                    exp, colnames, cntList, datatype));
            EXP.accept(E);
            return E.getResult();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public String createExpression(String record, String tabname,
            String condition, String[] colnames, int[] cntList,
            String[] datatype) {
        int count = 0;
        for (String s : colnames) {
            String val = record.split("\\|")[cntList[count]];

            if (datatype[count].equals("date")) {
                condition = condition.replaceAll("\\bDATE\\b\\('", "{d'");
                condition = condition.replaceAll("\\bdate\\b\\('", "{d'");
                Pattern regex = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}')(\\))");
                Matcher regexMatcher = regex.matcher(condition);
                condition = regexMatcher.replaceAll("$1\\}"); // *3 ??
                String date2 = "{d'" + val + "'}";
                condition = condition.replace(s, date2);
            } else if (datatype[count].equalsIgnoreCase("string")) {
                condition = condition.replace(s, "'" + val + "'");
            } else {
                condition = condition.replace(s, val);
            }
            count++;
        }
        return condition;
    }

    void getSingleExpressions() {

        LinkedHashSet<String> tab_list = new LinkedHashSet<String>();
        LinkedHashSet<String> col_list = new LinkedHashSet<String>();
        LinkedHashSet<String> colnames;
        for (Expression w : where_exp) {
            ExpressionEvaluator e1 = new ExpressionEvaluator();
            w.accept(e1);
            colnames = e1.getColNames();
            for (String tab : colnames) {
                if (tab.contains(".")) {
                    tab_list.add(tab.split("\\.")[0]);
                    col_list.add(tab);
                }

            }

            if (tab_list.size() == 1) {
                String tabname = tab_list.toArray()[0].toString();
                if (tabinfo.containsKey(tabname)) {
                    TableExpInfo t = tabinfo.get(tabname);
                    HashSet<String> col = t.getCol();
                    for (Object s : col_list.toArray()) {
                        col.add(s.toString());
                    }
                    t.setCol(col);
                    String exp = t.getExp() + " AND " + w.toString();
                    t.setExp(exp);
                    if (w.toString().contains(" OR ")) {
                        t.getAndOrList()[0].add(null);
                        t.getAndOrList()[1] = createORlist(w.toString());

                    } else {
                        Metadata m = new Metadata();
                        m.setCondition(w.toString());
                        t.getAndOrList()[0].add(m);

                    }

                    tabinfo.put(tabname, t);

                } else {
                    ArrayList<Metadata>[] mlist = new ArrayList[2];
                    mlist[0] = new ArrayList<Metadata>();
                    mlist[1] = new ArrayList<Metadata>();
                    TableExpInfo t = new TableExpInfo();
                    HashSet<String> col = new HashSet<String>();
                    for (Object s : col_list.toArray()) {
                        col.add(s.toString());
                    }

                    t.setCol(col);
                    t.setExp(w.toString());

                    if (w.toString().contains(" OR ")) {
                        mlist[0].add(null);
                        mlist[1] = createORlist(w.toString());
                    } else {
                        Metadata m = new Metadata();
                        m.setCondition(w.toString());
                        mlist[0].add(m);

                    }
                    t.setAndOrList(mlist);

                    tabinfo.put(tabname, t);

                }

            }
            tab_list.clear();
            col_list.clear();
        }

    }

    private ArrayList<Metadata> createORlist(String or) {

        ArrayList<Metadata> orList = new ArrayList<Metadata>();
        or = or.replaceAll("\\(", "");
        or = or.replaceAll("\\)", "");

        String[] temp = or.split(" OR ");
        for (String s : temp) {
            Metadata m = new Metadata();
            m.setCondition(s);
            orList.add(m);
        }

        return orList;
    }

    public void printtabinfo() {
        for (String t : tabinfo.keySet()) {

            System.out.println("TABLENAME: " + t);
            System.out.println();
            System.out.println("ANDLIST");

            for (Metadata m : tabinfo.get(t).getAndOrList()[0]) {
                if (m != null) {
                    m.printMetadata();
                }
            }
            System.out.println();
            System.out.println("ORLIST");

            for (Metadata m : tabinfo.get(t).getAndOrList()[1]) {

                m.printMetadata();

            }

            System.out.println("-----------------------");
        }
    }
}
