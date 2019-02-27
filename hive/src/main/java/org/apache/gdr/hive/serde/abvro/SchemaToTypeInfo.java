package org.apache.gdr.hive.serde.abvro;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.avro.InstanceCache;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.*;

import static org.apache.avro.Schema.Type.*;

/**
 * Convert an Avro Schema to a Hive TypeInfo
 */
class SchemaToTypeInfo {
    // Conversion of Avro primitive types to Hive primitive types
    // Avro             Hive
    // Null
    // boolean          boolean    check
    // int              int        check
    // long             bigint     check
    // float            double     check
    // double           double     check
    // bytes            binary     check
    // fixed            binary     check
    // string           string     check
    //                  tinyint
    //                  smallint

    // Map of Avro's primitive types to Hives (for those that are supported by both)
    private static final Map<Schema.Type, TypeInfo> primitiveTypeToTypeInfo = initTypeMap();
    static InstanceCache<Schema, TypeInfo> typeInfoCache = new InstanceCache<Schema, TypeInfo>() {
        @Override
        protected TypeInfo makeInstance(Schema s,
                                        Set<Schema> seenSchemas)
                throws AvroSerdeException {
            return generateTypeInfoWorker(s, seenSchemas);
        }
    };

    private static Map<Schema.Type, TypeInfo> initTypeMap() {
        Map<Schema.Type, TypeInfo> theMap = new Hashtable<Schema.Type, TypeInfo>();
        theMap.put(NULL, TypeInfoFactory.getPrimitiveTypeInfo("void"));
        theMap.put(BOOLEAN, TypeInfoFactory.getPrimitiveTypeInfo("boolean"));
        theMap.put(INT, TypeInfoFactory.getPrimitiveTypeInfo("int"));
        theMap.put(LONG, TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
        theMap.put(FLOAT, TypeInfoFactory.getPrimitiveTypeInfo("float"));
        theMap.put(DOUBLE, TypeInfoFactory.getPrimitiveTypeInfo("double"));
        theMap.put(BYTES, TypeInfoFactory.getPrimitiveTypeInfo("binary"));
        theMap.put(FIXED, TypeInfoFactory.getPrimitiveTypeInfo("binary"));
        theMap.put(STRING, TypeInfoFactory.getPrimitiveTypeInfo("string"));
        return Collections.unmodifiableMap(theMap);
    }

    /**
     * Generate a list of of TypeInfos from an Avro schema.  This method is
     * currently public due to some weirdness in deserializing unions, but
     * will be made private once that is resolved.
     *
     * @param schema Schema to generate field types for
     * @return List of TypeInfos, each element of which is a TypeInfo derived
     * from the schema.
     * @throws AvroSerdeException for problems during conversion.
     */
    public static List<TypeInfo> generateColumnTypes(Schema schema) throws AvroSerdeException {
        return generateColumnTypes(schema, null);
    }

    /**
     * Generate a list of of TypeInfos from an Avro schema.  This method is
     * currently public due to some weirdness in deserializing unions, but
     * will be made private once that is resolved.
     *
     * @param schema      Schema to generate field types for
     * @param seenSchemas stores schemas processed in the parsing done so far,
     *                    helping to resolve circular references in the schema
     * @return List of TypeInfos, each element of which is a TypeInfo derived
     * from the schema.
     * @throws AvroSerdeException for problems during conversion.
     */
    public static List<TypeInfo> generateColumnTypes(Schema schema,
                                                     Set<Schema> seenSchemas) throws AvroSerdeException {
        List<Schema.Field> fields = schema.getFields();

        List<TypeInfo> types = new ArrayList<TypeInfo>(fields.size());

        for (Schema.Field field : fields) {
            types.add(generateTypeInfo(field.schema(), seenSchemas));
        }

        return types;
    }

    /**
     * Convert an Avro Schema into an equivalent Hive TypeInfo.
     *
     * @param schema      to record. Must be of record type.
     * @param seenSchemas stores schemas processed in the parsing done so far,
     *                    helping to resolve circular references in the schema
     * @return TypeInfo matching the Avro schema
     * @throws AvroSerdeException for any problems during conversion.
     */
    public static TypeInfo generateTypeInfo(Schema schema,
                                            Set<Schema> seenSchemas) throws AvroSerdeException {
        // For bytes type, it can be mapped to decimal.
        Schema.Type type = schema.getType();
        if (type == BYTES && AbvroSerDe.DECIMAL_TYPE_NAME
                .equalsIgnoreCase(schema.getProp(AbvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
            int precision = 0;
            int scale = 0;
            try {
                precision = schema.getJsonProp(AbvroSerDe.AVRO_PROP_PRECISION).getIntValue();
                scale = schema.getJsonProp(AbvroSerDe.AVRO_PROP_SCALE).getIntValue();
            } catch (Exception ex) {
                throw new AvroSerdeException("Failed to obtain scale value from file schema: " + schema, ex);
            }

            try {
                HiveDecimalUtils.validateParameter(precision, scale);
            } catch (Exception ex) {
                throw new AvroSerdeException("Invalid precision or scale for decimal type", ex);
            }

            return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
        }

        if (type == STRING &&
                AbvroSerDe.CHAR_TYPE_NAME.equalsIgnoreCase(schema.getProp(AbvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
            int maxLength = 0;
            try {
                maxLength = schema.getJsonProp(AbvroSerDe.AVRO_PROP_MAX_LENGTH).getValueAsInt();
            } catch (Exception ex) {
                throw new AvroSerdeException("Failed to obtain maxLength value from file schema: " + schema, ex);
            }
            return TypeInfoFactory.getCharTypeInfo(maxLength);
        }

        if (type == STRING && AbvroSerDe.VARCHAR_TYPE_NAME
                .equalsIgnoreCase(schema.getProp(AbvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
            int maxLength = 0;
            try {
                maxLength = schema.getJsonProp(AbvroSerDe.AVRO_PROP_MAX_LENGTH).getValueAsInt();
            } catch (Exception ex) {
                throw new AvroSerdeException("Failed to obtain maxLength value from file schema: " + schema, ex);
            }
            return TypeInfoFactory.getVarcharTypeInfo(maxLength);
        }

        if (type == INT &&
                AbvroSerDe.DATE_TYPE_NAME.equals(schema.getProp(AbvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
            return TypeInfoFactory.dateTypeInfo;
        }

        if (type == LONG &&
                AbvroSerDe.TIMESTAMP_TYPE_NAME.equals(schema.getProp(AbvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
            return TypeInfoFactory.timestampTypeInfo;
        }

        return typeInfoCache.retrieve(schema, seenSchemas);
    }

    private static TypeInfo generateTypeInfoWorker(Schema schema,
                                                   Set<Schema> seenSchemas) throws AvroSerdeException {
        // Avro requires NULLable types to be defined as unions of some type T
        // and NULL.  This is annoying and we're going to hide it from the user.
        if (AvroSerdeUtils.isNullableType(schema)) {
            return generateTypeInfo(
                    AvroSerdeUtils.getOtherTypeFromNullableType(schema), seenSchemas);
        }

        Schema.Type type = schema.getType();
        if (primitiveTypeToTypeInfo.containsKey(type)) {
            return primitiveTypeToTypeInfo.get(type);
        }

        switch (type) {
            case RECORD:
                return generateRecordTypeInfo(schema, seenSchemas);
            case MAP:
                return generateMapTypeInfo(schema, seenSchemas);
            case ARRAY:
                return generateArrayTypeInfo(schema, seenSchemas);
            case UNION:
                return generateUnionTypeInfo(schema, seenSchemas);
            case ENUM:
                return generateEnumTypeInfo(schema);
            default:
                throw new AvroSerdeException("Do not yet support: " + schema);
        }
    }

    private static TypeInfo generateRecordTypeInfo(Schema schema,
                                                   Set<Schema> seenSchemas) throws AvroSerdeException {
        assert schema.getType().equals(Schema.Type.RECORD);

        if (seenSchemas == null) {
            seenSchemas = Collections.newSetFromMap(new IdentityHashMap<Schema, Boolean>());
        } else if (seenSchemas.contains(schema)) {
            return primitiveTypeToTypeInfo.get(Schema.Type.NULL);
        }
        seenSchemas.add(schema);

        List<Schema.Field> fields = schema.getFields();
        List<String> fieldNames = new ArrayList<String>(fields.size());
        List<TypeInfo> typeInfos = new ArrayList<TypeInfo>(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            fieldNames.add(i, fields.get(i).name());
            typeInfos.add(i, generateTypeInfo(fields.get(i).schema(), seenSchemas));
        }

        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    /**
     * Generate a TypeInfo for an Avro Map.  This is made slightly simpler in that
     * Avro only allows maps with strings for keys.
     */
    private static TypeInfo generateMapTypeInfo(Schema schema,
                                                Set<Schema> seenSchemas) throws AvroSerdeException {
        assert schema.getType().equals(Schema.Type.MAP);
        Schema valueType = schema.getValueType();
        TypeInfo ti = generateTypeInfo(valueType, seenSchemas);

        return TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("string"), ti);
    }

    private static TypeInfo generateArrayTypeInfo(Schema schema,
                                                  Set<Schema> seenSchemas) throws AvroSerdeException {
        assert schema.getType().equals(Schema.Type.ARRAY);
        Schema itemsType = schema.getElementType();
        TypeInfo itemsTypeInfo = generateTypeInfo(itemsType, seenSchemas);

        return TypeInfoFactory.getListTypeInfo(itemsTypeInfo);
    }

    private static TypeInfo generateUnionTypeInfo(Schema schema,
                                                  Set<Schema> seenSchemas) throws AvroSerdeException {
        assert schema.getType().equals(Schema.Type.UNION);
        List<Schema> types = schema.getTypes();


        List<TypeInfo> typeInfos = new ArrayList<TypeInfo>(types.size());

        for (Schema type : types) {
            typeInfos.add(generateTypeInfo(type, seenSchemas));
        }

        return TypeInfoFactory.getUnionTypeInfo(typeInfos);
    }

    // Hive doesn't have an Enum type, so we're going to treat them as Strings.
    // During the deserialize/serialize stage we'll check for enumness and
    // convert as such.
    private static TypeInfo generateEnumTypeInfo(Schema schema) {
        assert schema.getType().equals(Schema.Type.ENUM);

        return TypeInfoFactory.getPrimitiveTypeInfo("string");
    }
}
