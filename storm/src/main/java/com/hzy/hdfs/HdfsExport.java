package com.hzy.hdfs;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

/**
 * Created by zy on 2016/1/7.
 */
public class HdfsExport {
    public HdfsBolt HdfsConfig(){
        // Configure HDFS bolt
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("\t"); // use "\t" instead of "," for field delimiter
        SyncPolicy syncPolicy = new CountSyncPolicy(1000); // sync the filesystem after every 1k tuples
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES); // rotate files
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/storm/").withPrefix("app_").withExtension(".log"); // set file name format
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://master:8020/ksh")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        return hdfsBolt;
    }
}
