package org.apache.gdr.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

public abstract class AbInputFormat<K, V> extends FileInputFormat<K, V> implements JobConfigurable {
    protected JobConf jobConf;

    @Override
    final protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public void configure(JobConf jobConf) {
        this.jobConf = jobConf;
    }
}
