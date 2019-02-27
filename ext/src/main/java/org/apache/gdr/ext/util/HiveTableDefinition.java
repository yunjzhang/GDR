package org.apache.gdr.ext.util;

import org.apache.gdr.common.exception.GdrRuntimeException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.*;

import java.util.*;

public class HiveTableDefinition {
    static final int CREATE_TABLE = 0; // regular CREATE TABLE
    static final int CTLT = 1; // CREATE TABLE LIKE ... (CTLT)
    static final int CTAS = 2; // CREATE TABLE AS SELECT ... (CTAS)
    String likeTableName = null;
    String tableName = null;
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> bucketCols = new ArrayList<String>();
    List<Order> sortCols = new ArrayList<Order>();
    int numBuckets = -1;
    String comment = null;
    String location = null;
    Map<String, String> tblProps = null;
    boolean ifNotExists = false;
    boolean isExt = false;
    boolean isTemporary = false;
    ASTNode selectStmt = null;
    int command_type = CREATE_TABLE;
    List<String> skewedColNames = new ArrayList<String>();
    List<List<String>> skewedValues = new ArrayList<List<String>>();
    Map<List<String>, String> listBucketColValuesMapping = new HashMap<List<String>, String>();
    boolean storedAsDirs = false;
    boolean isUserStorageFormat = false;
    RowFormatParams rowFormatParams = new RowFormatParams();
    HiveConf conf = new HiveConf();
    StorageFormat storageFormat = new StorageFormat(conf);
    ASTNode ast;

    public HiveTableDefinition(String ddlString) throws SemanticException, ParseException {
        ast = ddl2Tree(ddlString);
        analyzeDDL(ast);
    }

    public HiveTableDefinition(ASTNode tree) throws SemanticException, ParseException {
        ast = tree;
        analyzeDDL(ast);
    }

    protected void analyzeDDL(ASTNode ast) throws SemanticException {
        int numCh = ast.getChildCount();

        //get tablename
        ASTNode child = (ASTNode) ast.getChild(0).getChild(0);
        tableName = child.getText();

        for (int num = 1; num < numCh; num++) {
            child = (ASTNode) ast.getChild(num);
            if (storageFormat.fillStorageFormat(child)) {
                isUserStorageFormat = true;
                continue;
            }
            switch (child.getToken().getType()) {
                case HiveParser.TOK_IFNOTEXISTS:
                    ifNotExists = true;
                    break;
                case HiveParser.KW_EXTERNAL:
                    isExt = true;
                    break;
                case HiveParser.KW_TEMPORARY:
                    isTemporary = true;
                    break;
                case HiveParser.TOK_LIKETABLE:
                    if (child.getChildCount() > 0) {
                        likeTableName = SemanticAnalyzer.getUnescapedName((ASTNode) child.getChild(0));
                        if (likeTableName != null) {
                            if (command_type == CTAS) {
                                throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE
                                        .getMsg());
                            }
                            if (cols.size() != 0) {
                                throw new SemanticException(ErrorMsg.CTLT_COLLST_COEXISTENCE
                                        .getMsg());
                            }
                            throw new GdrRuntimeException("'create table like table' is not supported currently");
                        }
                        command_type = CTLT;
                    }
                    break;
                case HiveParser.TOK_QUERY: // CTAS
                    if (command_type == CTLT) {
                        throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
                    }
                    if (cols.size() != 0) {
                        throw new SemanticException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
                    }
                    if (partCols.size() != 0 || bucketCols.size() != 0) {
                        boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING);
                        if (dynPart == false) {
                            throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
                        } else {
                            // TODO: support dynamic partition for CTAS
                            throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
                        }
                    }
                    if (isExt) {
                        throw new SemanticException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
                    }
                    command_type = CTAS;
                    selectStmt = child;
                    //break;
                    throw new GdrRuntimeException("'create table as' is not supported currently");
                case HiveParser.TOK_TABCOLLIST:
                    cols = SemanticAnalyzer.getColumns(child, true);
                    break;
                case HiveParser.TOK_TABLECOMMENT:
                    comment = SemanticAnalyzer.unescapeSQLString(child.getChild(0).getText());
                    break;
                case HiveParser.TOK_TABLEPARTCOLS:
                    partCols = SemanticAnalyzer.getColumns((ASTNode) child.getChild(0), false);
                    break;
                case HiveParser.TOK_ALTERTABLE_BUCKETS:
                    bucketCols = SemanticAnalyzer.getColumnNames((ASTNode) child.getChild(0));
                    if (child.getChildCount() == 2) {
                        numBuckets = (Integer.valueOf(child.getChild(1).getText()))
                                .intValue();
                    } else {
                        sortCols = getColumnNamesOrder((ASTNode) child.getChild(1));
                        numBuckets = (Integer.valueOf(child.getChild(2).getText()))
                                .intValue();
                    }
                    break;
                case HiveParser.TOK_TABLEROWFORMAT:
                    rowFormatParams.analyzeRowFormat(child);
                    break;
                case HiveParser.TOK_TABLELOCATION:
                    location = SemanticAnalyzer.unescapeSQLString(child.getChild(0).getText());
                    location = EximUtil.relativeToAbsolutePath(conf, location);
                    //inputs.add(SemanticAnalyzer.toReadEntity(location));
                    break;
                case HiveParser.TOK_TABLEPROPERTIES:
                    tblProps = new LinkedHashMap<>();
                    DDLSemanticAnalyzer.readProps((ASTNode) child.getChild(0), tblProps);
                    break;
                case HiveParser.TOK_TABLESERIALIZER:
                    child = (ASTNode) child.getChild(0);
                    storageFormat.setSerde(SemanticAnalyzer.unescapeSQLString(child.getChild(0).getText()));
                    if (child.getChildCount() == 2) {
                        DDLSemanticAnalyzer.readProps((ASTNode) (child.getChild(1).getChild(0)),
                                storageFormat.getSerdeProps());
                    }
                    break;
				/*
			case HiveParser.TOK_TABLESKEWED:

				// skewed column names
				skewedColNames = SemanticAnalyzer.analyzeSkewedTablDDLColNames(skewedColNames, child);
				// skewed value
				SemanticAnalyzer.analyzeDDLSkewedValues(skewedValues, child);
				// stored as directories
				storedAsDirs = SemanticAnalyzer.analyzeStoredAdDirs(child);

				break;
				 */
                default:
                    throw new AssertionError("Unknown token: " + child.getToken());
            }
        }
        //TODO run query on DB and get definition
		/*
		if (command_type == CTAS && selectStmt != null) {
			ASTNode nextRoot = (ASTNode) selectStmt.getChild(1).getChild(1);
			analyzeDDL(nextRoot);
		}
		*/
    }

    public Map<String, String> getTblProps() {
        return tblProps;
    }

    public ASTNode ddl2Tree(String sql) throws ParseException, SemanticException {
        if (StringUtils.isBlank(sql))
            return null;
        else {
            ParseDriver pd = new ParseDriver();
            HiveConf conf = new HiveConf();
            String command = new VariableSubstitution().substitute(conf, sql);
            ASTNode t = pd.parse(command);
            t = ParseUtils.findRootNonNullToken(t);
            return t;
        }
    }

    public ASTNode getAst() {
        return ast;
    }

    public void setAst(ASTNode ast) {
        this.ast = ast;
    }

    public List<FieldSchema> getCols() {
        return cols;
    }

    public void setCols(List<FieldSchema> cols) {
        this.cols = cols;
    }

    protected List<Order> getColumnNamesOrder(ASTNode ast) {
        List<Order> colList = new ArrayList<Order>();
        int numCh = ast.getChildCount();
        for (int i = 0; i < numCh; i++) {
            ASTNode child = (ASTNode) ast.getChild(i);
            if (child.getToken().getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
                colList.add(new Order(SemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText()).toLowerCase(),
                        SemanticAnalyzer.HIVE_COLUMN_ORDER_ASC));
            } else {
                colList.add(new Order(SemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText()).toLowerCase(),
                        SemanticAnalyzer.HIVE_COLUMN_ORDER_DESC));
            }
        }
        return colList;
    }

    public Boolean validateCols() {
        return validateCols(cols);
    }

    public Boolean validateCols(List<FieldSchema> cols) {
        if (cols == null || cols.size() < 1)
            return false;

        Set<String> colNameSet = new HashSet<>();
        for (FieldSchema col : cols) {
            if (!colNameSet.add(col.getName()))
                return false;
        }
        return true;
    }

    public String getFieldDelim() {
        return rowFormatParams.fieldDelim != null ? rowFormatParams.fieldDelim : storageFormat.getSerdeProps().get("field.delim");
    }

    public String getLineDelim() {
        return rowFormatParams.fieldDelim != null ? rowFormatParams.lineDelim : storageFormat.getSerdeProps().get("line.delim");
    }

    public String getInputFormat() {
        return storageFormat.getInputFormat();
    }

    public String getOutputFormat() {
        return storageFormat.getOutputFormat();
    }

    public String getSerde() {
        return storageFormat.getSerde();
    }

    public String getTableName() {
        return tableName;
    }

    class RowFormatParams {
        String fieldDelim = null;
        String fieldEscape = null;
        String collItemDelim = null;
        String mapKeyDelim = null;
        String lineDelim = null;
        String nullFormat = null;

        protected void analyzeRowFormat(ASTNode child) throws SemanticException {
            child = (ASTNode) child.getChild(0);
            int numChildRowFormat = child.getChildCount();
            for (int numC = 0; numC < numChildRowFormat; numC++) {
                ASTNode rowChild = (ASTNode) child.getChild(numC);
                switch (rowChild.getToken().getType()) {
                    case HiveParser.TOK_TABLEROWFORMATFIELD:
                        fieldDelim = SemanticAnalyzer.unescapeSQLString(rowChild.getChild(0)
                                .getText());
                        if (rowChild.getChildCount() >= 2) {
                            fieldEscape = SemanticAnalyzer.unescapeSQLString(rowChild
                                    .getChild(1).getText());
                        }
                        break;
                    case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
                        collItemDelim = SemanticAnalyzer.unescapeSQLString(rowChild
                                .getChild(0).getText());
                        break;
                    case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
                        mapKeyDelim = SemanticAnalyzer.unescapeSQLString(rowChild.getChild(0)
                                .getText());
                        break;
                    case HiveParser.TOK_TABLEROWFORMATLINES:
                        lineDelim = SemanticAnalyzer.unescapeSQLString(rowChild.getChild(0)
                                .getText());
                        if (!lineDelim.equals("\n")
                                && !lineDelim.equals("10")) {
                            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(rowChild,
                                    ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg()));
                        }
                        break;
                    case HiveParser.TOK_TABLEROWFORMATNULL:
                        nullFormat = SemanticAnalyzer.unescapeSQLString(rowChild.getChild(0)
                                .getText());
                        break;
                    default:
                        throw new AssertionError("Unkown Token: " + rowChild);
                }
            }
        }
    }
}
