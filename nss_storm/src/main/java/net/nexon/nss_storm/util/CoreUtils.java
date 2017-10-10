package net.nexon.nss_storm.util;

import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CoreUtils {
    private static DateFormat dtSourceFormat  = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSS'Z'");

    private static String GetDateString(Date date, String format) {
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(date);
    }

    public static String GetFullPath(String msg, String fieldName) {
        JSONObject js = new JSONObject(msg);
        Object dtVal = js.get(fieldName);
        Date date = null;
        String path = "";
        try {
            date = CoreUtils.dtSourceFormat.parse(dtVal.toString());
            path = String.format("/%s", CoreUtils.GetDateString(date, "yyyy/MM/dd"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return path;
    }
}
