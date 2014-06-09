
package edu.buffalo.cse562;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

import java.io.File;
import java.io.FileReader;

public class ReadSql {

    public static void parseFile(File sql_file)
    {

        try {
            FileReader read = new FileReader(sql_file);

            CCJSqlParser parse = new CCJSqlParser(read);

            Statement s;

            while ((s = parse.Statement()) != null) {

                StatementEvaluator e = new StatementEvaluator();
                s.accept(e);
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
