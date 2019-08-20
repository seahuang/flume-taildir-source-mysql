/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source.taildir.mysql;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class TailFile {
  private static final Logger logger = LoggerFactory.getLogger(TailFile.class);

  private static final byte BYTE_NL = (byte) 10;
  private static final byte BYTE_CR = (byte) 13;

  private static final int BUFFER_SIZE = 8192;
  private static final int NEED_READING = -1;

  private RandomAccessFile raf;
  private final String path;
  private final long inode;
  private long pos;
  private long lastUpdated;
  private boolean needTail;
  private final Map<String, String> headers;
  private byte[] buffer;
  private byte[] oldBuffer;
  private int bufferPos;
  private long lineReadPos;

  public TailFile(File file, Map<String, String> headers, long inode, long pos)
      throws IOException {
    this.raf = new RandomAccessFile(file, "r");
    if (pos > 0) {
      raf.seek(pos);
      lineReadPos = pos;
    }
    this.path = file.getAbsolutePath();
    this.inode = inode;
    this.pos = pos;
    this.lastUpdated = 0L;
    this.needTail = true;
    this.headers = headers;
    this.oldBuffer = new byte[0];
    this.bufferPos = NEED_READING;
  }

  public RandomAccessFile getRaf() {
    return raf;
  }

  public String getPath() {
    return path;
  }

  public long getInode() {
    return inode;
  }

  public long getPos() {
    return pos;
  }

  public long getLastUpdated() {
    return lastUpdated;
  }

  public boolean needTail() {
    return needTail;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public long getLineReadPos() {
    return lineReadPos;
  }

  public void setPos(long pos) {
    this.pos = pos;
  }

  public void setLastUpdated(long lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  public void setNeedTail(boolean needTail) {
    this.needTail = needTail;
  }

  public void setLineReadPos(long lineReadPos) {
    this.lineReadPos = lineReadPos;
  }

  public boolean updatePos(String path, long inode, long pos) throws IOException {
    if (this.inode == inode && this.path.equals(path)) {
      setPos(pos);
      updateFilePos(pos);
      logger.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos);
      return true;
    }
    return false;
  }
  public void updateFilePos(long pos) throws IOException {
    raf.seek(pos);
    lineReadPos = pos;
    bufferPos = NEED_READING;
    oldBuffer = new byte[0];
  }


  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL,
      boolean addByteOffset) throws IOException {
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent(backoffWithoutNL, addByteOffset);
      if (event == null) {
        break;
      }
      events.add(event);
    }
    return events;
  }

  private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
    Long posTmp = getLineReadPos();
    LineResult line = readLine();
    if (line == null) {
      return null;
    }
    logger.debug("readEvent line="+new String(line.line));
    if (backoffWithoutNL && !line.lineSepInclude) {
      logger.info("Backing off in file without newline: "
          + path + ", inode: " + inode + ", pos: " + raf.getFilePointer());
      updateFilePos(posTmp);
      return null;
    }
    Event event = EventBuilder.withBody(line.line);
    if (addByteOffset == true) {
      event.getHeaders().put(TaildirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY, posTmp.toString());
    }
    return event;
  }

  public List<Event> readMysqlSlowLogEvents(int numEvents, boolean backoffWithoutNL,
                                            boolean addByteOffset) throws IOException {
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readMysqlSlowLogEvent(backoffWithoutNL, addByteOffset);
      if (event == null) {
        break;
      }
      events.add(event);
    }
    return events;
  }

  private Map<String,Object> getLocalNetworkInfo() {
    InetAddress ia=null;
    Map<String,Object> map = new HashMap<String,Object>();
    try {
      ia=ia.getLocalHost();
      String localname=ia.getHostName();
      String localip=ia.getHostAddress();
      map.put("server_ip", localip);
      map.put("server_name", localname);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return map;
  }

  private Event readMysqlSlowLogEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
    Long posTmp = getLineReadPos();
    ConcurrentHashMap<String,Object> cacheItem = TailFileCache.getOrInitGavaCache(path);

    StringBuffer sbTime, sbUserHost, sbQueryTime, sbUse, sbSetTimestamp, sbSql, allLog;
    while(true) {
      sbTime = (StringBuffer) cacheItem.get("line_time");
      sbUserHost = (StringBuffer) cacheItem.get("line_user_host");
      sbQueryTime = (StringBuffer) cacheItem.get("line_query_time");
      sbUse = (StringBuffer) cacheItem.get("line_use");
      sbSetTimestamp = (StringBuffer) cacheItem.get("line_set_timestamp");
      allLog = (StringBuffer) cacheItem.get("allLog");
      if (allLog == null) {
        cacheItem.put("allLog", (allLog = new StringBuffer()));
      }
      // read file
      LineResult line = readLine();
      if (line == null) {
        logger.debug("readEvent["+path+"] null "+allLog.toString());
        return null;
      }
      String lineStr = new String(line.line);
      //# Time: 190326 20:42:27
      if (lineStr.startsWith("# Time:")) {
        cacheItem.put("line_time", (sbTime = new StringBuffer(lineStr)));
        continue;
      }
      //# User@Host: root[root] @ localhost []  Id: 51696
      if (lineStr.startsWith("# User@Host:")) {
        //start
        {
          cacheItem.clear();
          sbUserHost = (StringBuffer) cacheItem.get("line_user_host");
          sbQueryTime = (StringBuffer) cacheItem.get("line_query_time");
          sbUse = (StringBuffer) cacheItem.get("line_use");
          sbSetTimestamp = (StringBuffer) cacheItem.get("line_set_timestamp");
          allLog = (StringBuffer) cacheItem.get("allLog");
          if (allLog == null) {
            cacheItem.put("allLog", (allLog = new StringBuffer()));
          }
        }
        if (sbTime!=null) {
          // 有 #Time: ...情况
          Map<String, Object> resMap = TaildirMysqlSlowLogParser.parseTime(sbTime.toString());
          if (resMap == null) {
            logger.warn("readEvent["+path+"] Time pattern failed. line="+sbTime.toString());
          } else {
            cacheItem.put("line_time", sbTime);
            cacheItem.putAll(resMap);
            cacheItem.put("allLog", allLog.append(sbTime.toString()).append("\r\n"));
          }
        }
        //
        Map<String,Object> networkMap = getLocalNetworkInfo();
        Map<String, Object> resMap = TaildirMysqlSlowLogParser.parseUserHost(lineStr);
        if (resMap == null) {
          logger.error("readEvent["+path+"] User@Host pattern failed. line="+lineStr);
          continue;
        }
        cacheItem.putAll(resMap);
        cacheItem.putAll(networkMap);

        cacheItem.put("line_user_host", (sbUserHost = new StringBuffer(lineStr)));
        cacheItem.put("allLog", allLog.append(lineStr).append("\r\n"));

//        logger.debug("readEvent["+path+"] "+allLog.toString());
        continue;
      }
      //# Query_time: 39.794432  Lock_time: 0.000418 Rows_sent: 1  Rows_examined: 36103478
      if (lineStr.startsWith("# Query_time:")) {
        Map<String, Object> resMap = TaildirMysqlSlowLogParser.parseQueryTime(lineStr);
        if (resMap == null) {
          logger.error("readEvent["+path+"] Query_time pattern failed. line="+lineStr);
          continue;
        }
        cacheItem.putAll(resMap);

        cacheItem.put("line_query_time", (sbQueryTime = new StringBuffer(lineStr)));
        cacheItem.put("allLog", allLog.append(lineStr).append("\r\n"));

//        logger.debug("readEvent["+path+"] "+allLog.toString());
        continue;
      }
      //use vvmobileliveaccount;
      if (Pattern.matches("use \\w+;", lineStr)) {
        Map<String, Object> resMap = TaildirMysqlSlowLogParser.parseUse(lineStr);
        if (resMap == null) {
          logger.error("readEvent["+path+"] use pattern failed. line="+lineStr);
          continue;
        }
        cacheItem.putAll(resMap);

        cacheItem.put("line_use", (sbUse = new StringBuffer(lineStr)));
        cacheItem.put("allLog", allLog.append(lineStr).append("\r\n"));

//        logger.debug("readEvent["+path+"] "+allLog.toString());
        continue;
      }
      //SET timestamp=1553604147;
      if (Pattern.matches("SET timestamp=\\d+;", lineStr)) {
        Map<String, Object> resMap = TaildirMysqlSlowLogParser.parseSetTimestamp(lineStr);
        if (resMap == null) {
          logger.error("readEvent["+path+"] use pattern failed. line="+lineStr);
          continue;
        }
        cacheItem.putAll(resMap);

        cacheItem.put("line_set_timestamp", (sbSetTimestamp = new StringBuffer(lineStr)));
        cacheItem.put("allLog", allLog.append(lineStr).append("\r\n"));

//        logger.debug("readEvent["+path+"] "+allLog.toString());
        continue;
      }

      if (sbUserHost!=null && sbQueryTime!=null
              && sbSetTimestamp!=null) {
        sbSql = (StringBuffer) cacheItem.get("sql");
        if (sbSql==null) {
          cacheItem.put("sql", sbSql = new StringBuffer());
        }
        if (!lineStr.endsWith(";")) {
          // SQL
          cacheItem.put("sql", sbSql.append(lineStr).append("\r\n"));
          cacheItem.put("allLog", allLog.append(lineStr).append("\r\n"));
          continue;
        }
        // SQL ending
        cacheItem.put("sql", sbSql.append(lineStr));
        cacheItem.put("allLog", allLog.append(lineStr).append("\r\n"));

        // create event
        cacheItem.remove("allLog");
        Gson gson = new Gson();
        String jsonMsg = gson.toJson(cacheItem);
        logger.debug("readEvent["+this.getPath()+"] [sql end] "+jsonMsg);

        Event event = EventBuilder.withBody(jsonMsg.getBytes());
        if (addByteOffset == true) {
          event.getHeaders().put(TaildirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY, posTmp.toString());
        }
        // clear
        cacheItem.clear();
        // return
        return event;
      }

//      if (backoffWithoutNL && !lineSepInclude) {
//        logger.info("Backing off in file without newline: "
//                + path + ", inode: " + inode + ", pos: " + raf.getFilePointer());
//        updateFilePos(posTmp);
//        return null;
//      }
    }

  }

  private void readFile() throws IOException {
    if ((raf.length() - raf.getFilePointer()) < BUFFER_SIZE) {
      buffer = new byte[(int) (raf.length() - raf.getFilePointer())];
    } else {
      buffer = new byte[BUFFER_SIZE];
    }
    raf.read(buffer, 0, buffer.length);
    bufferPos = 0;
  }

  private byte[] concatByteArrays(byte[] a, int startIdxA, int lenA,
                                  byte[] b, int startIdxB, int lenB) {
    byte[] c = new byte[lenA + lenB];
    System.arraycopy(a, startIdxA, c, 0, lenA);
    System.arraycopy(b, startIdxB, c, lenA, lenB);
    return c;
  }

  public LineResult readLine() throws IOException {
    LineResult lineResult = null;
    while (true) {
      if (bufferPos == NEED_READING) {
        if (raf.getFilePointer() < raf.length()) {
          readFile();
        } else {
          if (oldBuffer.length > 0) {
            lineResult = new LineResult(false, oldBuffer);
            oldBuffer = new byte[0];
            setLineReadPos(lineReadPos + lineResult.line.length);
          }
          break;
        }
      }
      for (int i = bufferPos; i < buffer.length; i++) {
        if (buffer[i] == BYTE_NL) {
          int oldLen = oldBuffer.length;
          // Don't copy last byte(NEW_LINE)
          int lineLen = i - bufferPos;
          // For windows, check for CR
          if (i > 0 && buffer[i - 1] == BYTE_CR) {
            lineLen -= 1;
          } else if (oldBuffer.length > 0 && oldBuffer[oldBuffer.length - 1] == BYTE_CR) {
            oldLen -= 1;
          }
          lineResult = new LineResult(true,
              concatByteArrays(oldBuffer, 0, oldLen, buffer, bufferPos, lineLen));
          setLineReadPos(lineReadPos + (oldBuffer.length + (i - bufferPos + 1)));
          oldBuffer = new byte[0];
          if (i + 1 < buffer.length) {
            bufferPos = i + 1;
          } else {
            bufferPos = NEED_READING;
          }
          break;
        }
      }
      if (lineResult != null) {
        break;
      }
      // NEW_LINE not showed up at the end of the buffer
      oldBuffer = concatByteArrays(oldBuffer, 0, oldBuffer.length,
                                   buffer, bufferPos, buffer.length - bufferPos);
      bufferPos = NEED_READING;
    }
    return lineResult;
  }

  public void close() {
    try {
      raf.close();
      raf = null;
      long now = System.currentTimeMillis();
      setLastUpdated(now);
    } catch (IOException e) {
      logger.error("Failed closing file: " + path + ", inode: " + inode, e);
    }
  }

  private class LineResult {
    final boolean lineSepInclude;
    final byte[] line;

    public LineResult(boolean lineSepInclude, byte[] line) {
      super();
      this.lineSepInclude = lineSepInclude;
      this.line = line;
    }
  }
}
