package org.apache.gdr.spark.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.IOException;
import java.util.List;

public class SparkSqlParser {
	static Log LOG = LogFactory.getLog(SparkSqlParser.class);

	//TODO
	public static String generateDML(ASTNode root) {
		return null;
	}

	//TODO
	public static String generateGdr(ASTNode root) {
		return null;
	}

	public static void main(String[] args) throws ParseException, IOException, SemanticException {
		String command = "create table t /*commit*/ (c1 int, c2 string, c3 decimal(18,0), c4 varchar(100))";
		//String command = "create table t as select c1, c2, c3 from t0";
		CreateTableDefinition ctd = new CreateTableDefinition(command);
		ASTNode t = ctd.getAst();
		printNode(t, 1);
		
		if (!ctd.validateCols())
			System.out.println("dup column find in ddl.");
		
		List<FieldSchema> cols = ctd.getCols();
		for (FieldSchema c : cols) {
			System.out.println(c.toString());
		}
	}

	static void printNode(ASTNode t, int level) {
		if (t != null) {
			for (int i = 0; i < level; i++) 
				System.out.print("\t");

			System.out.println(t.getToken().getText() + " : " + t.getToken().getType());
			List<? extends Node> l = t.getChildren();
			if (l == null || l.size() < 1)
				return;
			for (Node c : l) {
				printNode((ASTNode) c, level+1);
			}
		}
	}
}
