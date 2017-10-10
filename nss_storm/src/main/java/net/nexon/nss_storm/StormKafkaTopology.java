package net.nexon.nss_storm;

import java.util.UUID;

import net.nexon.nss_storm.bolt.format.HdfsBoltExt;
import net.nexon.nss_storm.bolt.format.HdfsBoltExt2;
import net.nexon.nss_storm.bolt.format.JsonEpochTimeRecordFormat;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import net.nexon.nss_storm.bolt.format.YmdPartitionedFileNameFormat;
import org.apache.storm.tuple.Fields;

public class StormKafkaTopology {

    public static void main(String[] args) throws AlreadyAliveException,
            InvalidTopologyException, InterruptedException,
            AuthorizationException {

        // This class expects 3 arguments in this order:
        // HDFS Host, Kafka Topic, HDFS Output Directory

        //if(args.length != 3)
        //{
        //System.out.println("Incorrect number of input arguments!");
        //System.exit(1);
        //}

        //String hostname = args[0];
        //String kafkaTopic = args[1];
        //String hdfsOutputDir = args[2];

        String hostname = "ip-10-30-10-141.us-west-2.compute.internal";
        String kafkaTopic = "mantis-event-queue1";
        String hdfsOutputDir = "hslee_storm_test";

        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(2.0f, Units.GB);
        RecordFormat format = new JsonEpochTimeRecordFormat().withEpochTimeField("received_time");

        YmdPartitionedFileNameFormat fileNameFormat = new YmdPartitionedFileNameFormat()
                .withPrefix("test-20-")
                .withExtension(".csv")
                .withPath(hdfsOutputDir);

        HdfsBoltExt2 hdfsbolt = new HdfsBoltExt2()
                .withFsUrl("hdfs://" + hostname + ":8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        BrokerHosts hosts = new ZkHosts("ip-10-30-10-141.us-west-2.compute.internal:2181,ip-10-30-10-167.us-west-2.compute.internal:2181,ip-10-30-10-192.us-west-2.compute.internal:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, "/" + kafkaTopic,
                "storm-kafka-group");

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", kafkaSpout, 1).setNumTasks(1);
        builder.setBolt("hdfs-bolt", hdfsbolt,2)
                .setNumTasks(2)
                .shuffleGrouping("kafka-spout");

        Config conf = new Config();

    /*
     * LocalCluster cluster = new LocalCluster();
     * cluster.submitTopology("test", conf, builder.createTopology());
     * Utils.sleep(10000); cluster.killTopology("test"); cluster.shutdown();
    */
        conf.setNumWorkers(1);
        StormSubmitter.submitTopology("test-topology-3-20", conf,
                builder.createTopology());
    }
}