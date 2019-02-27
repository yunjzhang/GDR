package org.apache.gdr.hive.ql.io.ab;

import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

public abstract class AbOutputFormat<K, V> implements HiveOutputFormat<K, V>, JobConfigurable {
    protected JobConf jobConf;

    @Override
    public void configure(JobConf jobConf) {
        this.jobConf = jobConf;
    }

}
