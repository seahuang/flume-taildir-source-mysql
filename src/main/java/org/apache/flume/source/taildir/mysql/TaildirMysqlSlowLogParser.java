package org.apache.flume.source.taildir.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by huanghai on 2019/8/18.
 */
public class TaildirMysqlSlowLogParser {
    private static final Logger logger = LoggerFactory.getLogger(TaildirMysqlSlowLogParser.class);

    public static Map<String, Object> parseTime(String lineStr) {
        HashMap<String, Object> retMap = null;
        Pattern pattern = Pattern.compile("# Time: (\\d+)[ ]+(\\d{1,2}:\\d{1,2}:\\d{1,2})");
        Matcher matcher = pattern.matcher(lineStr);
        if (!matcher.find()) {
            return null;
        }
        if (matcher.groupCount() != 2) {
            return null;
        }
        retMap = new HashMap<>();
        retMap.put("time_dt", matcher.group(1));
        retMap.put("time_hms", matcher.group(2));
        return retMap;
    }

    public static Map<String, Object> parseUserHost(String lineStr) {
        HashMap<String, Object> retMap = null;
        Pattern pattern = Pattern.compile("# User@Host: (\\w+\\[\\w+\\]) @ (\\w*)[ ]+\\[(.*)\\][ ]+Id: (\\d+)");
        Matcher matcher = pattern.matcher(lineStr);
        if (!matcher.find()) {
            return null;
        }
        if (matcher.groupCount() != 4) {
            return null;
        }
        retMap = new HashMap<>();
        retMap.put("db_user", matcher.group(1));
        retMap.put("db_client_name", matcher.group(2));
        retMap.put("db_client_ip", matcher.group(3));
        retMap.put("db_id", Long.parseLong(matcher.group(4)));
        return retMap;
    }

    public static Map<String, Object> parseQueryTime(String lineStr) {
        HashMap<String, Object> retMap = null;
        Pattern pattern = Pattern.compile("# Query_time: (\\d+\\.\\d+)[ ]+Lock_time: (\\d+\\.\\d+)[ ]+Rows_sent: (\\d+)[ ]+Rows_examined: (\\d+)");
        Matcher matcher = pattern.matcher(lineStr);
        if (!matcher.find()) {
            return null;
        }
        if (matcher.groupCount() != 4) {
            return null;
        }
        retMap = new HashMap<>();
        retMap.put("query_time", Double.parseDouble(matcher.group(1))*1000);
        retMap.put("lock_time", Double.parseDouble(matcher.group(2))*1000);
        retMap.put("rows_sent", Long.parseLong(matcher.group(3)));
        retMap.put("rows_examined", Long.parseLong(matcher.group(4)));

        return retMap;
    }

    public static Map<String, Object> parseUse(String lineStr) {
        HashMap<String, Object> retMap = null;
        Pattern pattern = Pattern.compile("use[ ]+(\\w+);");
        Matcher matcher = pattern.matcher(lineStr);
        if (!matcher.find()) {
            return null;
        }
        if (matcher.groupCount() != 1) {
            return null;
        }
        retMap = new HashMap<>();
        retMap.put("db_name", matcher.group(1));

        return retMap;
    }

    public static Map<String, Object> parseSetTimestamp(String lineStr) {
        HashMap<String, Object> retMap = null;
        Pattern pattern = Pattern.compile("SET[ ]+timestamp=(\\d+);");
        Matcher matcher = pattern.matcher(lineStr);
        if (!matcher.find()) {
            return null;
        }
        if (matcher.groupCount() != 1) {
            return null;
        }
        retMap = new HashMap<>();
        long timeInMillis = Long.parseLong(matcher.group(1));
        retMap.put("timestamp", timeInMilli2UTC(timeInMillis*1000));

        return retMap;
    }

    public static String timeInMilli2UTC(long timeInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        Date date = cal.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        String dtStr = sdf.format(date);
        return dtStr;
    }
}
