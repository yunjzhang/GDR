package org.apache.gdr.hive.serde.abvro;

import org.apache.gdr.common.util.DmlParser;
import org.apache.gdr.mapred.conf.Constant;
import org.apache.gdr.mapred.util.ContextTool;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.avro.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Read or write Avro data from Hive.
 */
@SerDeSpec(schemaProps = {
        serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
        AbvroSerDe.LIST_COLUMN_COMMENTS, AbvroSerDe.TABLE_NAME, AbvroSerDe.TABLE_COMMENT,
        AvroSerdeUtils.SCHEMA_LITERAL, AvroSerdeUtils.SCHEMA_URL,
        AvroSerdeUtils.SCHEMA_NAMESPACE, AvroSerdeUtils.SCHEMA_NAME, AvroSerdeUtils.SCHEMA_DOC})
public class AbvroSerDe extends AbstractSerDe {
    public static final String TABLE_NAME = "name";
    public static final String TABLE_COMMENT = "comment";
    public static final String LIST_COLUMN_COMMENTS = "columns.comments";
    public static final String DECIMAL_TYPE_NAME = "decimal";
    public static final String CHAR_TYPE_NAME = "char";
    public static final String VARCHAR_TYPE_NAME = "varchar";
    public static final String DATE_TYPE_NAME = "date";
    public static final String TIMESTAMP_TYPE_NAME = "timestamp-millis";
    public static final String AVRO_PROP_LOGICAL_TYPE = "logicalType";
    public static final String AVRO_PROP_PRECISION = "precision";
    public static final String AVRO_PROP_SCALE = "scale";
    public static final String AVRO_PROP_MAX_LENGTH = "maxLength";
    public static final String AVRO_STRING_TYPE_NAME = "string";
    public static final String AVRO_INT_TYPE_NAME = "int";
    public static final String AVRO_LONG_TYPE_NAME = "long";
    private static final Log LOG = LogFactory.getLog(AbvroSerDe.class);
    private ObjectInspector oi;
    private List<String> columnNames;
    private List<TypeInfo> columnTypes;
    private Schema schema;
    private AbvroDeserializer avroDeserializer = null;
    private AbvroSerializer avroSerializer = null;

    private boolean badSchema = false;

    public static Schema getSchemaFromCols(Properties properties,
                                           List<String> columnNames, List<TypeInfo> columnTypes, String columnCommentProperty) {
        List<String> columnComments;
        if (columnCommentProperty == null || columnCommentProperty.isEmpty()) {
            columnComments = new ArrayList<String>();
        } else {
            //Comments are separated by "\0" in columnCommentProperty, see method getSchema
            //in MetaStoreUtils where this string columns.comments is generated
            columnComments = Arrays.asList(columnCommentProperty.split("\0"));
            LOG.debug("columnComments is " + columnCommentProperty);
        }
        if (columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("AvroSerde initialization failed. Number of column " +
                    "name and column type differs. columnNames = " + columnNames + ", columnTypes = " +
                    columnTypes);
        }

        final String tableName = properties.getProperty(TABLE_NAME);
        final String tableComment = properties.getProperty(TABLE_COMMENT);
        TypeInfoToSchema typeInfoToSchema = new TypeInfoToSchema();
        return typeInfoToSchema.convert(columnNames, columnTypes, columnComments,
                properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_NAMESPACE.getPropName()),
                properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_NAME.getPropName(), tableName),
                properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_DOC.getPropName(), tableComment));

    }

    @Override
    public void initialize(Configuration configuration, Properties tableProperties,
                           Properties partitionProperties) throws SerDeException {
        // Avro should always use the table properties for initialization (see HIVE-6835).
        initialize(configuration, tableProperties);
    }

    @Override
    public void initialize(Configuration configuration, Properties properties) throws SerDeException {
        // Reset member variables so we don't get in a half-constructed state
        if (schema != null) {
            LOG.debug("Resetting already initialized AvroSerDe");
        }

        schema = null;
        oi = null;
        columnNames = null;
        columnTypes = null;

        final String columnNameProperty = properties.getProperty(serdeConstants.LIST_COLUMNS);
        final String columnTypeProperty = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        final String columnCommentProperty = properties.getProperty(LIST_COLUMN_COMMENTS, "");

        if (properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()) != null
                || properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName()) != null
                || properties.getProperty(Constant.AB_DML_FILE) != null
                || columnNameProperty == null || columnNameProperty.isEmpty()
                || columnTypeProperty == null || columnTypeProperty.isEmpty()) {
            String dmlURL = "";
            FileSystem fs = null;
            try {
                //configuration is null in spark 2.1, add extra step to pull from Properties
                if (configuration == null) {
                    configuration = new Configuration();
                    for (Entry<Object, Object> entry : properties.entrySet()) {
                        String key = entry.getKey().toString();
                        if (key.startsWith("hadoop.conf")) {
                            configuration.set(entry.getKey().toString().replaceFirst("hadoop.conf", ""), entry.getValue().toString());
                        }
                    }
                }

                dmlURL = properties.getProperty(Constant.AB_DML_FILE);
                fs = FileSystem.get(new URI(dmlURL), configuration);
                Boolean ignoreDummyColumn = Boolean.valueOf(
                        properties.getProperty(Constant.AB_DML_IGNORE_DUMMY_COLUMN, "false"));
                Boolean enableLogicType = !Boolean.valueOf(
                        properties.getProperty(Constant.AB_DATETIME_AS_STRING, "false"));
                schema = DmlParser.parseDml(ContextTool.readDmlFile(new Path(dmlURL), fs))
                        .genAvroSchema(ignoreDummyColumn, enableLogicType);
            } catch (Exception e) {
                e.printStackTrace();
                //LOG.error("FAILED to get dml file from: " + dmlURL);
                LOG.debug("configuration is null?: " + configuration);
                throw new RuntimeException("FAILED to get dml file from: " + dmlURL);
            }
        } else {
            // Get column names and sort order
            columnNames = Arrays.asList(columnNameProperty.split(","));
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

            schema = getSchemaFromCols(properties, columnNames, columnTypes, columnCommentProperty);
            properties.setProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), schema.toString());
        }

        LOG.debug("Avro schema is " + schema);

        if (configuration == null) {
            LOG.info("Configuration null, not inserting schema");
        } else {
            configuration.set(
                    AvroSerdeUtils.AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName(), schema.toString(false));
        }

        badSchema = schema.equals(org.apache.gdr.hive.serde.abvro.SchemaResolutionProblem.SIGNAL_BAD_SCHEMA);

        AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(schema);
        this.columnNames = aoig.getColumnNames();
        this.columnTypes = aoig.getColumnTypes();
        this.oi = aoig.getObjectInspector();
    }

    /**
     * Attempt to determine the schema via the usual means, but do not throw
     * an exception if we fail.  Instead, signal failure via a special
     * schema.  This is used because Hive calls init on the serde during
     * any call, including calls to update the serde properties, meaning
     * if the serde is in a bad state, there is no way to update that state.
     */
    public Schema determineSchemaOrReturnErrorSchema(Configuration conf, Properties props) {
        try {
            configErrors = "";
            return AvroSerdeUtils.determineSchemaOrThrowException(conf, props);
        } catch (AvroSerdeException he) {
            LOG.warn("Encountered AvroSerdeException determining schema. Returning " +
                    "signal schema to indicate problem", he);
            configErrors = new String("Encountered AvroSerdeException determining schema. Returning " +
                    "signal schema to indicate problem: " + he.getMessage());
            return schema = org.apache.gdr.hive.serde.abvro.SchemaResolutionProblem.SIGNAL_BAD_SCHEMA;
        } catch (Exception e) {
            LOG.warn("Encountered exception determining schema. Returning signal " +
                    "schema to indicate problem", e);
            configErrors = new String("Encountered exception determining schema. Returning signal " +
                    "schema to indicate problem: " + e.getMessage());
            return org.apache.gdr.hive.serde.abvro.SchemaResolutionProblem.SIGNAL_BAD_SCHEMA;
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return AvroGenericRecordWritable.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        if (badSchema) {
            throw new org.apache.gdr.hive.serde.abvro.BadSchemaException();
        }
        return getSerializer().serialize(o, objectInspector, columnNames, columnTypes, schema);
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        if (badSchema) {
            throw new BadSchemaException();
        }
        return getDeserializer().deserialize(columnNames, columnTypes, writable, schema);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return oi;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // No support for statistics. That seems to be a popular answer.
        return null;
    }

    private AbvroDeserializer getDeserializer() {
        if (avroDeserializer == null) {
            avroDeserializer = new AbvroDeserializer();
        }

        return avroDeserializer;
    }

    private AbvroSerializer getSerializer() {
        if (avroSerializer == null) {
            avroSerializer = new AbvroSerializer();
        }

        return avroSerializer;
    }
}
