
package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;
import java.util.List;

public class DataType {

    String colName;
    List<Object> col_list;

    public DataType(String colName) {
        this.colName = colName;
        this.col_list = new ArrayList<Object>();
    }

    /**
     * @return DataType as String (string, long, double, date)
     */
    @SuppressWarnings("unchecked")
    public String getDatatype() {
        if (colName.contains(".")) {

            colName = colName.split("\\.")[1];
        }
        String datatype = new String();
        boolean b = false;
        for (CreateTable ct : Main.ct) {
            col_list = ct.getColumnDefinitions();
            for (Object col : col_list) {
                if (col.toString().contains(colName)) {
                    datatype = col.toString().split(" ")[1];
                    b = true;
                    break;
                }
                if (b)
                    break;
            }
        }

        if (datatype.contains("char") || datatype.contains("string") || datatype.contains("CHAR")
                || datatype.contains("STRING")) {
            return "string";
        } else if (datatype.contains("int") || datatype.contains("INT")) {
            return "long";
        } else if (datatype.contains("decimal") || datatype.contains("DECIMAL")) {
            return "double";
        } else if (datatype.contains("date") || datatype.contains("DATE")) {
            return "date";
        }
        return "double";
    }

}
