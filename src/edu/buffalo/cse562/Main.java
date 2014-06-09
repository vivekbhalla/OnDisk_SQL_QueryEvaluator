
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Main {
    public static String data_dir = null;
    public static String swap_dir = null;
    public static List<Select> select = new LinkedList<Select>();
    public static List<CreateTable> ct = new ArrayList<CreateTable>();
    public static long startTime;

    public static void main(String[] args) throws ParseException {

        startTime = System.currentTimeMillis();
        List<File> sql_files = new ArrayList<File>();

        for (int i = 0; i < args.length; i++) {

            if (args[i].equals("--data")) {
                data_dir = args[i + 1];
                i++;
            } else if (args[i].equals("--swap")) {
                swap_dir = args[i + 1];
                i++;
            } else {
                sql_files.add(new File(args[i]));
            }
        }

        for (File sql : sql_files) {

            ReadSql.parseFile(sql);
            if (select.size() > 0) {
                new Evaluation().evaluate_sql();
            }
            select = new ArrayList<Select>();
            // ct = new ArrayList<CreateTable>();

        }
        //
        // long endTime = System.currentTimeMillis();
        // long totalTime = endTime - Main.startTime;
        // System.out.println("Time After Completion: " + totalTime / 1000
        // + " seconds");

        // long total = Runtime.getRuntime().totalMemory();
        // long free = Runtime.getRuntime().freeMemory();
        // long used = total - free;
        // System.out.println("Used memory in MB after Completion: " + used
        // / (1024 * 1024) + "MB");
    }

    public static Expression parseGeneralExpression(String exprStr)
            throws ParseException {
        CCJSqlParser parser = new CCJSqlParser(new StringReader(exprStr));

        return parser.Expression();
    }

    public static Expression parseArithematicExpression(String exprStr)
            throws ParseException {
        CCJSqlParser parser = new CCJSqlParser(new StringReader(exprStr));
        return parser.AdditiveExpression();
    }
}
