package net.nexon.nss_storm.bolt.format;

import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

// DelimitedRecordFormat
public class JsonEpochTimeRecordFormat implements RecordFormat {
    private String targetField = "";
    private DateFormat dtSource  = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSS'Z'");
    // yyyy-mm-dd hh:mm:ss
    private DateFormat dtDest = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public JsonEpochTimeRecordFormat() {
    }

    public JsonEpochTimeRecordFormat withEpochTimeField(String targetField) {
        this.targetField = targetField;
        return this;
    }

    /*
    {
      "received_time": "20171009T192710.838Z",
      "target_version": "",
      "event_type": "game_in_progress",
      "client_ip": "68.15.90.201",
      "user_no": 25883128,
      "source_version": "",
      "cmts": "{}",
      "product_id": "10000",
      "install_type": "",
      "patcher_type": "",
      "channel_id": "nxa",
      "user_agent": "NexonLauncher.nxl-17.07.02-164-312d35d",
      "device_id": "4ffb4990f33ce49fc7164ee27574aadb6c228804db2fac5a3c9d47643d3205a4"
    }
    */

    public byte[] format(Tuple tuple) {
        StringBuilder sb = new StringBuilder();
        String jsonString = tuple.getString(0);
        JSONObject js = new JSONObject(jsonString);

        sb.append("{");
        boolean bNeedDelimiter = false;

        // POC code
        for (Object key : js.keySet()) {
            String keyStr = (String) key;
            Object keyvalue = js.get(keyStr);

            if (bNeedDelimiter) {
                sb.append(",");
                bNeedDelimiter = false;
            }

            if (keyStr.compareTo(this.targetField) == 0) {
                try {
                    Date date = dtSource.parse(keyvalue.toString());
                    //  yyyy-mm-dd hh:mm:ss[.fffffffff] for hive
                    sb.append(String.format("\"%s\": \"%s\"", keyStr, dtDest.format(date)));
                    // epochTime
                    //sb.append(String.format("\"%s\": %d", keyStr, date.getTime()/1000 ));
                    bNeedDelimiter = true;
                } catch (ParseException e) {
                    sb.append(String.format("\"error:%s\": \"%s\"", keyStr, e.toString()));
                }
            }
            else {
                sb.append(String.format("\"%s\": \"%s\"", keyStr, keyvalue.toString()));
                bNeedDelimiter = true;
            }
        }
        sb.append("}");
        return sb.toString().getBytes();
    }
}
