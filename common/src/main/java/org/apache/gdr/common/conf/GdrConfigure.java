package org.apache.gdr.common.conf;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class GdrConfigure implements ConfigureInterface, Serializable {
    private static final long serialVersionUID = -7672822710461109516L;

    private final Map<String, Object> conf = new HashMap<>();

    public static ConfigureInterface create() {
        ConfigureInterface configure = new GdrConfigure();
        configure.setConfigure(Property.DSS_JAVA_TS_FORMAT, Constant.DSS_JAVA_TS_FORMAT);
        configure.setConfigure(Property.DSS_JAVA_DT_FORMAT, Constant.DSS_JAVA_DT_FORMAT);
        configure.setConfigure(Property.DSS_AB_TS_FORMAT, Constant.DSS_AB_TS_FORMAT);
        configure.setConfigure(Property.DSS_AB_DT_FORMAT, Constant.DSS_AB_DT_FORMAT);
        configure.setConfigure(Property.DSS_DEFAULT_COL_DELIMITER, Constant.DSS_DEFAULT_COL_DELIMITER);
        configure.setConfigure(Property.DSS_DEFAULT_LINE_DELIMITER, Constant.DSS_DEFAULT_LINE_DELIMITER);
        configure.setConfigure(Property.DSS_DEFAULT_ENDIAN, Constant.DSS_DEFAULT_ENDIAN);
        configure.setConfigure(Property.IGNORE_COLUMN_NAME, Constant.IGNORE_COLUMN_NAME);
        configure.setConfigure(Property.AB_DEFAULT_CHARSET, Constant.AB_DEFAULT_CHARSET);
        configure.setConfigure(Property.DSS_ETL_TIMEZONE, Constant.DSS_ETL_TIMEZONE);
        configure.setConfigure(Property.DSS_DEFAULT_SCHEMA_NAMESPACE, Constant.DSS_DEFAULT_SCHEMA_NAMESPACE);
        configure.setConfigure(Property.IGNORE_BROKEN_RECORD, Constant.IGNORE_BROKEN_RECORD);
        configure.setConfigure(Property.DSS_DEFAULT_DECIMAL_PRECISION, Constant.DSS_DEFAULT_DECIMAL_PRECISION);
        configure.setConfigure(Property.DSS_DEFAULT_DECIMAL_SCALE, Constant.DSS_DEFAULT_DECIMAL_SCALE);
        return configure;
    }

    @Override
    public Integer getIntegerConfigure(String key) {
        return (Integer) getConfigure(key);
    }

    @Override
    public String getStringConfigure(String key) {
        Object value = getConfigure(key);
        if (value == null)
            return "";
        return new String(Base64.getDecoder().decode(((String) getConfigure(key)).getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.ISO_8859_1);
    }

    @Override
    public Long getLongConfigure(String key) {
        return (Long) getConfigure(key);
    }

    @Override
    public Double getDoubleConfigure(String key) {
        return (Double) getConfigure(key);
    }

    @Override
    public Boolean getBooleanConfigure(String key) {
        return (Boolean) getConfigure(key);
    }

    @Override
    public void setConfigure(String key, Object value) {
        if (value == null)
            value = "";

        if (value instanceof String) {
            String encodeStr = Base64.getEncoder().encodeToString(
                    value.toString().getBytes(StandardCharsets.UTF_8));
            conf.put(key, encodeStr);
        } else {
            conf.put(key, value);
        }
    }

    @Override
    public Object getConfigure(String key) {
        return conf.get(key);
    }
}
