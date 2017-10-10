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


public class HdfsBoltExt2 extends AbstractHdfsBolt {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.storm.hdfs.bolt.HdfsBolt.class);
    private transient FSDataOutputStream out;
    private RecordFormat format;
    private String boltId;

    private Map<String, FSDataOutputStream> outputMap = new HashMap<String, FSDataOutputStream>();

    public HdfsBoltExt2() {
    }

    public HdfsBoltExt2 withFsUrl(String fsUrl) {
        this.fsUrl = fsUrl;
        return this;
    }

    public HdfsBoltExt2 withConfigKey(String configKey) {
        this.configKey = configKey;
        return this;
    }

    public HdfsBoltExt2 withFileNameFormat(FileNameFormat fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
        return this;
    }

    public HdfsBoltExt2 withRecordFormat(RecordFormat format) {
        this.format = format;
        return this;
    }

    public HdfsBoltExt2 withSyncPolicy(SyncPolicy syncPolicy) {
        this.syncPolicy = syncPolicy;
        return this;
    }

    public HdfsBoltExt2 withRotationPolicy(FileRotationPolicy rotationPolicy) {
        this.rotationPolicy = rotationPolicy;
        return this;
    }

    public HdfsBoltExt2 addRotationAction(RotationAction action) {
        this.rotationActions.add(action);
        return this;
    }

    public HdfsBoltExt2 withTickTupleIntervalSeconds(int interval) {
        this.tickTupleInterval = Integer.valueOf(interval);
        return this;
    }

    public HdfsBoltExt2 withRetryCount(int fileRetryCount) {
        this.fileRetryCount = Integer.valueOf(fileRetryCount);
        return this;
    }

    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.boltId = topologyContext.getThisComponentId();
        LOG.info("[hslee] Bolt ID: " + this.boltId);
        this.fs = FileSystem.get(URI.create(this.fsUrl), this.hdfsConfig);
    }

    protected void syncTuples(FSDataOutputStream out) throws IOException {
        LOG.debug("Attempting to sync all data to filesystem");
        if(out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream)out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            out.hsync();
        }
    }

    protected void syncTuples() throws IOException {
        LOG.debug("Attempting to sync all data to filesystem");
        if(this.out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream)this.out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            this.out.hsync();
        }

        //
        for (Object value : this.outputMap.values()) {
            this.syncTuples((FSDataOutputStream)value);
        }
    }

    protected FSDataOutputStream GetOutputStream(Tuple tuple) {
        // get path
        String msg = tuple.getString(0);
        String subPath = CoreUtils.GetFullPath(msg, "received_time");
        String key = this.fileNameFormat.getPath() + subPath;

        if (this.outputMap.get(key) == null) {
            try {
                Path path = new Path(key, this.fileNameFormat.getName((long)this.rotation, System.currentTimeMillis()));
                FSDataOutputStream out = this.fs.create(path);
                this.outputMap.put(key, out);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return this.outputMap.get(key);
    }

    protected void writeTuple(Tuple tuple) throws IOException {
        // get path
        FSDataOutputStream out = GetOutputStream(tuple);
        if (out != null) {
            byte[] bytes = this.format.format(tuple);
            //this.out.write(bytes);
            out.write(bytes);
            String cr = String.format("%s\n", "");
            out.write(cr.getBytes());
            this.offset += (long) bytes.length;
        }
    }

    protected void closeOutputFile() throws IOException {
        this.out.close();
        //
        for (Object value : this.outputMap.values()) {
            FSDataOutputStream fs = (FSDataOutputStream)value;
            fs.close();
        }
        this.outputMap.clear();
    }

    protected Path createOutputFile() throws IOException {
        Path path = new Path(this.fileNameFormat.getPath() + "/tmp", this.fileNameFormat.getName((long)this.rotation, System.currentTimeMillis()));
        this.out = this.fs.create(path);
        return path;
    }
}
