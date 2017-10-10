package net.nexon.nss_storm.bolt.format;

import net.nexon.nss_storm.util.CoreUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.storm.hdfs.bolt.AbstractHdfsBolt;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.EnumSet;
import java.util.Map;


public class HdfsBoltExt extends AbstractHdfsBolt {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.storm.hdfs.bolt.HdfsBolt.class);
    private transient FSDataOutputStream out;
    private RecordFormat format;
    private String boltId;

    public HdfsBoltExt() {
    }

    public HdfsBoltExt withFsUrl(String fsUrl) {
        this.fsUrl = fsUrl;
        return this;
    }

    public HdfsBoltExt withConfigKey(String configKey) {
        this.configKey = configKey;
        return this;
    }

    public HdfsBoltExt withFileNameFormat(FileNameFormat fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public HdfsBoltExt withRecordFormat(RecordFormat format) {
        this.format = format;
        return this;
    }

    public HdfsBoltExt withSyncPolicy(SyncPolicy syncPolicy) {
        this.syncPolicy = syncPolicy;
        return this;
    }

    public HdfsBoltExt withRotationPolicy(FileRotationPolicy rotationPolicy) {
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public HdfsBoltExt addRotationAction(RotationAction action) {
        this.rotationActions.add(action);
        return this;
    }

    public HdfsBoltExt withTickTupleIntervalSeconds(int interval) {
        this.tickTupleInterval = Integer.valueOf(interval);
        return this;
    }

    public HdfsBoltExt withRetryCount(int fileRetryCount) {
        this.fileRetryCount = Integer.valueOf(fileRetryCount);
        return this;
    }

    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.boltId = topologyContext.getThisComponentId();
        LOG.info("[hslee] Bolt ID: " + this.boltId);
        this.fs = FileSystem.get(URI.create(this.fsUrl), this.hdfsConfig);
    }

    protected void syncTuples() throws IOException {
        LOG.debug("Attempting to sync all data to filesystem");
        if(this.out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream)this.out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            this.out.hsync();
        }
    }

    protected void writeTuple(Tuple tuple) throws IOException {
        byte[] bytes = this.format.format(tuple);
        this.out.write(bytes);
        this.offset += (long) bytes.length;
    }

    protected void closeOutputFile() throws IOException {
        this.out.close();
    }

    protected Path createOutputFile() throws IOException {
        Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName((long)this.rotation, System.currentTimeMillis()));
        this.out = this.fs.create(path);
        return path;
    }
}
