package net.nexon.nss_storm.bolt.format;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.TopologyContext;
import java.util.Map;
import java.util.Date;
import java.text.SimpleDateFormat;


public class YmdPartitionedFileNameFormat implements FileNameFormat {
    private String componentId;
    private int taskId;
    private String path = "/storm";
    private String prefix = "";
    private String extension = ".txt";

    public YmdPartitionedFileNameFormat() {
    }

    public YmdPartitionedFileNameFormat withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public YmdPartitionedFileNameFormat withExtension(String extension) {
        this.extension = extension;
        return this;
    }

    public YmdPartitionedFileNameFormat withPath(String path) {
        this.path = path;
        return this;
    }

    public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
    }

    public String getName(long rotation, long timeStamp) {
        return this.prefix + this.componentId + "-" + this.taskId + "-" + rotation + "-" + timeStamp + this.extension;
    }

    private String GetDateString(String format) {
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(new Date());
    }

    public String getPath() {
        return this.path
                + "/launcher3"
                + GetDateString("/yyyy")
                + GetDateString("/MM")
                + GetDateString("/dd");
    }
}
