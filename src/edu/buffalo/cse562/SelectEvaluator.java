package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

import java.util.List;

public class SelectEvaluator implements SelectVisitor {

    @Override
    public void visit(PlainSelect arg0) {
        Evaluation.plain_selects.add(arg0);

    }

    @SuppressWarnings("unchecked")
    @Override
    public void visit(Union arg0) {
        List<PlainSelect> ps=arg0.getPlainSelects();	
        for (PlainSelect p : ps)
            Evaluation.plain_selects.add(p);
    }

}
