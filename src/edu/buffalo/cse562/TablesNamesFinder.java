package edu.buffalo.cse562;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TablesNamesFinder implements FromItemVisitor, SelectVisitor {
    private List <Table>tables=new ArrayList<Table>();
    private SubSelect sub_select=null;
    
    public List<Table> getTableList(Select select) {
        tables = new ArrayList<Table>();
        select.getSelectBody().accept(this);
        return tables;
    }
    
    public  SubSelect getSubSelect() {
       
        return sub_select;
    }
    

    @Override
    public void visit(Table tableName) {
    	
        tables.add(tableName);
    }

    @Override
    public void visit(SubSelect subSelect) {
     sub_select=subSelect;
        subSelect.getSelectBody().accept(this);
    }

    @Override
    public void visit(SubJoin subjoin) {
    	
        subjoin.getLeft().accept(this);
        subjoin.getJoin().getRightItem().accept(this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void visit(PlainSelect plainSelect) {
        plainSelect.getFromItem().accept(this);

        if (plainSelect.getJoins() != null) {
            for (Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
                Join join = (Join) joinsIt.next();
                join.getRightItem().accept(this);
            }
        }
    }


    @SuppressWarnings("rawtypes")
    @Override
    public void visit(Union union) {
        for (Iterator iter = union.getPlainSelects().iterator(); iter.hasNext();) {
            PlainSelect plainSelect = (PlainSelect) iter.next();
            visit(plainSelect);
        }
    }



}