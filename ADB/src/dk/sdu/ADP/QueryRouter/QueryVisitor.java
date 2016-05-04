package dk.sdu.ADP.QueryRouter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.ADP.QueryType;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.UpdateDeParser;



public class QueryVisitor implements SelectVisitor, FromItemVisitor, ExpressionVisitor, ItemsListVisitor, StatementVisitor{

	private List<String> tables;
	private Map<String,String> column;
	private boolean flag; //Flag for whether to collect the columns and tables information
	private String par_name,par_value;
	
	public QueryVisitor(){
		tables = new ArrayList<String>();
		column = new HashMap<String,String>();
		this.flag = false;
		
		this.par_name = null;
		this.par_value = null;
	}
	
	private void init(){
		tables.clear();
		column.clear();
		this.par_name = null;
		this.par_value = null;
	}
	
	public QueryInfo getInfo(Select select) {
		
		this.flag = true;
		this.init();
		select.getSelectBody().accept(this);
		
		//System.out.println(newsql);
		
		return new QueryInfo(column, tables, ADP.QueryType.SELECT, select);
	}
	
	public QueryInfo getInfo(Delete delete) {
		
		this.flag = true;
		this.init();
		//String table = delete.getTable().getWholeTableName();
		delete.getTable().accept(this);
		
		//System.out.println(table);
		
		if(delete.getWhere() != null){
			Expression es = delete.getWhere();
			
//			EqualsTo eq = new EqualsTo();
//			eq.setLeftExpression(new Column(new Table(),par_name));
//			eq.setRightExpression(new LongValue(par_value));
			
			delete.getWhere().accept(this);
			
//			if(!flag && !column.containsKey(par_name)){
//				if(es instanceof AndExpression){
//					((BinaryExpression) es).setLeftExpression(new AndExpression(((BinaryExpression) es).getLeftExpression(), eq));
//				}else{
//					delete.setWhere(new AndExpression(es,eq));
//				}
//			
//			}
			
		}
		
		//System.out.println(newsql);
		
		return new QueryInfo(column, tables,ADP.QueryType.DELETE,delete);
	}
	
	
	public String setParCol(Statement stmt, String par_name, String par_value ){
		
		this.flag = false;
		this.par_name = par_name;
		this.par_value = par_value;
		String newSql = null;
		if(stmt instanceof Select){
			((Select) stmt).getSelectBody().accept(this);
			newSql = ((Select)stmt).toString();
		}else if(stmt instanceof Delete){
			Delete delete = (Delete) stmt;
			
			if(delete.getWhere() != null){
				Expression es = delete.getWhere();
				
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(new Column(new Table(),par_name));
				eq.setRightExpression(new LongValue(par_value));
				
				delete.getWhere().accept(this);
				
				if(!flag && !column.containsKey(par_name)){
					if(es instanceof AndExpression){
						((BinaryExpression) es).setLeftExpression(new AndExpression(((BinaryExpression) es).getLeftExpression(), eq));
					}else{
						delete.setWhere(new AndExpression(es,eq));
					}
				
					}
				
			}
			
			newSql = delete.toString();
			
		}else if(stmt instanceof Update){
			Update update = (Update) stmt;
			
			if(update.getWhere() != null){
				Expression es = update.getWhere();
				
				update.getWhere().accept(this);
				
				if(!flag && !column.containsKey(par_name)){
					
					EqualsTo eq = new EqualsTo();
					eq.setLeftExpression(new Column(new Table(),par_name));
					eq.setRightExpression(new LongValue(par_value));
					
					if(es instanceof AndExpression){
						((BinaryExpression) es).setLeftExpression(new AndExpression(((BinaryExpression) es).getLeftExpression(), eq));
					}else{
						update.setWhere(new AndExpression(es,eq));
					}
				}		
			}
			
			UpdateDeParser deup = new UpdateDeParser();
			StringBuffer buf = new StringBuffer();
			deup.setBuffer(buf);

			ExpressionDeParser expdep = new ExpressionDeParser(this,buf);
			deup.setExpressionVisitor(expdep);
			
			deup.deParse(update);
			
			newSql = buf.toString();
		
		}else if(stmt instanceof Insert){
			Insert insert = (Insert) stmt;
			
			if(insert.getColumns() != null){
				
				List icol = insert.getColumns();
				
				ExpressionList itcol = (ExpressionList) insert.getItemsList();
				
				if(!flag && !icol.contains(par_name)){
					
					Column nc = new Column(new Table(),par_name);
					LongValue nv = new LongValue(par_value);
					List vlist = itcol.getExpressions();
					vlist.add(nv);
					itcol.setExpressions(vlist);
					icol.add(nc);
					
					insert.setColumns(icol);
					insert.setItemsList(itcol);		
					
				}					
			}
			
			newSql = insert.toString();
			
		}
		
		
		return newSql;
	}
	
	public QueryInfo getInfo(Update update) {

		this.flag = true;
		this.init();
		
		update.getTable().accept(this);
		
		if(update.getWhere() != null){
			Expression es = update.getWhere();
			
			update.getWhere().accept(this);
			
//			if(!flag && !column.containsKey(par_name)){
//				
//				EqualsTo eq = new EqualsTo();
//				eq.setLeftExpression(new Column(new Table(),par_name));
//				eq.setRightExpression(new LongValue(par_value));
//				
//				if(es instanceof AndExpression){
//					((BinaryExpression) es).setLeftExpression(new AndExpression(((BinaryExpression) es).getLeftExpression(), eq));
//				}else{
//					update.setWhere(new AndExpression(es,eq));
//				}
//			}
			
			

		}
		
		return new QueryInfo(column, tables,ADP.QueryType.UPDATE,update);
	}
	
	public QueryInfo getInfo(Insert insert) {
		
		this.flag = true;
		this.init();
		
		insert.getTable().accept(this);
		
		if(insert.getColumns() != null){
			
			List icol = insert.getColumns();
			
			ExpressionList itcol = (ExpressionList) insert.getItemsList();
			
//			if(!flag && !icol.contains(par_name)){
//				
//				Column nc = new Column(new Table(),par_name);
//				LongValue nv = new LongValue(par_value);
//				List vlist = itcol.getExpressions();
//				vlist.add(nv);
//				itcol.setExpressions(vlist);
//				icol.add(nc);
//				
//				insert.setColumns(icol);
//				insert.setItemsList(itcol);		
//				
//			}
			
			
			for(Iterator it = icol.iterator(), vit = itcol.getExpressions().iterator();it.hasNext() && vit.hasNext();){
				column.put(it.next().toString(), vit.next().toString());
			}		
			
			
		}
		
		return new QueryInfo(column, tables,ADP.QueryType.INSERT,insert);
	}
	

	public void visit(PlainSelect plainSelect) {
		FromItem esfrom = plainSelect.getFromItem();
		esfrom.accept(this);
		
		if (plainSelect.getJoins() != null) {
			for (Iterator<Join> joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
				Join join = joinsIt.next();
				join.getRightItem().accept(this);
			}
		}
		if (plainSelect.getWhere() != null){
			Expression es = plainSelect.getWhere();
			
			plainSelect.getWhere().accept(this);
			
			if(!flag && !column.containsKey(par_name)){
				
				EqualsTo eq = new EqualsTo();
				eq.setLeftExpression(new Column(new Table(),par_name));
				eq.setRightExpression(new LongValue(par_value));
				
				if(es instanceof AndExpression){
					((BinaryExpression) es).setLeftExpression(new AndExpression(((BinaryExpression) es).getLeftExpression(), eq));
				}else{
					plainSelect.setWhere(new AndExpression(es,eq));
				}
			}
			
			

		}

	}
	

	public void visit(Union union) {
		for (Iterator<PlainSelect> iter = union.getPlainSelects().iterator(); iter.hasNext();) {
			PlainSelect plainSelect = iter.next();
			visit(plainSelect);
		}
	}

	public void visit(Table tableName) {
		String tableWholeName = tableName.getWholeTableName();
		if(flag)
			tables.add(tableWholeName);
	}

	public void visit(SubSelect subSelect) {
		subSelect.getSelectBody().accept(this);
	}

	public void visit(Addition addition) {
		visitBinaryExpression(addition);
	}

	public void visit(AndExpression andExpression) {
		visitBinaryExpression(andExpression);
	}

	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	public void visit(Column tableColumn) {
		String columnWholeName = tableColumn.getWholeColumnName();
		//column.add(columnWholeName);
	}

	public void visit(Division division) {
		visitBinaryExpression(division);
	}

	public void visit(DoubleValue doubleValue) {
	}

	public void visit(EqualsTo equalsTo) {
		visitBinaryExpression(equalsTo);
	}

	public void visit(Function function) {
	}

	public void visit(GreaterThan greaterThan) {
		visitBinaryExpression(greaterThan);
	}

	public void visit(GreaterThanEquals greaterThanEquals) {
		visitBinaryExpression(greaterThanEquals);
	}

	public void visit(InExpression inExpression) {
		inExpression.getLeftExpression().accept(this);
		inExpression.getItemsList().accept(this);
	}

	public void visit(InverseExpression inverseExpression) {
		inverseExpression.getExpression().accept(this);
	}

	public void visit(IsNullExpression isNullExpression) {
	}

	public void visit(JdbcParameter jdbcParameter) {
	}

	public void visit(LikeExpression likeExpression) {
		visitBinaryExpression(likeExpression);
	}

	public void visit(ExistsExpression existsExpression) {
		existsExpression.getRightExpression().accept(this);
	}

	public void visit(LongValue longValue) {
	}

	public void visit(MinorThan minorThan) {
		visitBinaryExpression(minorThan);
	}

	public void visit(MinorThanEquals minorThanEquals) {
		visitBinaryExpression(minorThanEquals);
	}

	public void visit(Multiplication multiplication) {
		visitBinaryExpression(multiplication);
	}

	public void visit(NotEqualsTo notEqualsTo) {
		visitBinaryExpression(notEqualsTo);
	}

	public void visit(NullValue nullValue) {
	}

	public void visit(OrExpression orExpression) {
		visitBinaryExpression(orExpression);
	}

	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}

	public void visit(StringValue stringValue) {
	}

	public void visit(Subtraction subtraction) {
		visitBinaryExpression(subtraction);
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		Expression left = binaryExpression.getLeftExpression();
		Expression right = binaryExpression.getRightExpression();
		if(flag && left instanceof Column){
			String r = right.toString();
			if(r.contains("'"))
				r = r.substring(r.indexOf("'") + 1, r.lastIndexOf("'"));
			column.put(((Column) left).getWholeColumnName(), r);
		}
		left.accept(this);
		right.accept(this);
	}

	public void visit(ExpressionList expressionList) {
		for (Iterator<Expression> iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
			Expression expression = iter.next();
			expression.accept(this);
		}

	}

	public void visit(DateValue dateValue) {
	}
	
	public void visit(TimestampValue timestampValue) {
	}
	
	public void visit(TimeValue timeValue) {
	}

	public void visit(CaseExpression caseExpression) {
	}

	public void visit(WhenClause whenClause) {
	}

	public void visit(AllComparisonExpression allComparisonExpression) {
		allComparisonExpression.GetSubSelect().getSelectBody().accept(this);
	}

	public void visit(AnyComparisonExpression anyComparisonExpression) {
		anyComparisonExpression.GetSubSelect().getSelectBody().accept(this);
	}

	public void visit(SubJoin subjoin) {
		subjoin.getLeft().accept(this);
		subjoin.getJoin().getRightItem().accept(this);
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Select arg0) {
		
		//Debug info
		System.out.println("In function visit(Select)");
		//End Debug
		
	}

	@Override
	public void visit(Delete arg0) {
		//Debug info
		System.out.println("In function visit(Delete)");
		//End Debug
		
		
	}

	@Override
	public void visit(Update arg0) {
		//Debug info
		System.out.println("In function visit(Update)");
		//End Debug
		
	}

	@Override
	public void visit(Insert arg0) {
		//Debug info
		System.out.println("In function visit(Insert)");
		//End Debug
		
	}

	@Override
	public void visit(Replace arg0) {
		//Debug info
		System.out.println("In function visit(Replace)");
		//End Debug
		
	}

	@Override
	public void visit(Drop arg0) {
		//Debug info
		System.out.println("In function visit(Drop)");
		//End Debug
		
	}

	@Override
	public void visit(Truncate arg0) {
		
		//Debug info
		System.out.println("In function visit(Truncate)");
		//End Debug
		
	}

	@Override
	public void visit(CreateTable arg0) {
		//Debug info
		System.out.println("In function visit(CreateTable)");
		//End Debug
		
	}




}
