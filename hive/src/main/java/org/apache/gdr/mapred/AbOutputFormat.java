package org.apache.gdr.mapred;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

public abstract class AbOutputFormat<K, V> extends FileOutputFormat<K, V> implements JobConfigurable {
    protected JobConf jobConf;

    @Override
    public void configure(JobConf jobConf) {
        this.jobConf = jobConf;
    }

}
