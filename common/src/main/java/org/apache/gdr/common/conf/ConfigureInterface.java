package org.apache.gdr.common.conf;

public interface ConfigureInterface {
    public Object getConfigure(String key);

    public Integer getIntegerConfigure(String key);

    public String getStringConfigure(String key);

    public Long getLongConfigure(String key);

    public Double getDoubleConfigure(String key);

    public Boolean getBooleanConfigure(String key);

    public void setConfigure(String key, Object Value);
}
