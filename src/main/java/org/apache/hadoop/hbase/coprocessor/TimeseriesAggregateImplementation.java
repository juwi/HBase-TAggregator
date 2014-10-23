package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateService;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * A concrete TimeseriesAggregateProtocol implementation. Its system level coprocessor that computes
 * the aggregate function at a region level. {@link ColumnInterpreter} is used to interpret column
 * value. This class is parameterized with the following (these are the types with which the
 * {@link ColumnInterpreter} is parameterized, and for more description on these, refer to
 * {@link ColumnInterpreter}):
 * @param <T> Cell value data type
 * @param <S> Promoted data type
 * @param <P> PB message that is used to transport initializer specific bytes
 * @param <Q> PB message that is used to transport Cell (<T>) instance
 * @param <R> PB message that is used to transport Promoted (<S>) instance
 **/
@InterfaceAudience.Private
public class TimeseriesAggregateImplementation<T, S, P extends Message, Q extends Message, R extends Message>
    extends TimeseriesAggregateService implements CoprocessorService, Coprocessor {
  protected static final Log log = LogFactory.getLog(AggregateImplementation.class);
  private RegionCoprocessorEnvironment env;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // TODO Implement Coprocessor.stop

  }

  private TimeRange getInitialTimeRange(TimeseriesAggregateRequest request, Scan scan) {
    try {
      long interval = request.getTimeIntervalSeconds();
      if (!request.hasRange()) {
        return new TimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMin() + interval
            * 1000l);
      } else {
        long tsmin = request.getRange().getKeyTimestampMin();
        return new TimeRange(tsmin * 1000l, (long) (tsmin + interval) * 1000l);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  private TimeRange getNextTimeRange(TimeRange timeRange, int interval) throws IOException {
    long intervalMillis = interval * 1000;
    return new TimeRange(timeRange.getMax(), timeRange.getMax() + intervalMillis);

  }

  private long getMillisTimestampFromOffset(long currentTimeStamp, int offset) {
    long offsetMicro = (long) offset * 1000l;
    return currentTimeStamp + offsetMicro;
  }

  private List<TimeRange> getAllTimeRanges(Scan scan, TimeseriesAggregateRequest request)
      throws IOException {
    List<TimeRange> timeRanges = new ArrayList<TimeRange>();
    Long max = getMaxTimeStamp(scan, request);
    TimeRange next = getInitialTimeRange(request, scan);
    timeRanges.add(next);
    do {
      next = getNextTimeRange(next, request.getTimeIntervalSeconds());
      timeRanges.add(next);
    } while (next.getMax() <= max);
    return timeRanges;
  }

  private int getTimestampFromRowKeyAsSeconds(Cell kv, TimeseriesAggregateRequest request) {
    String keyPattern = request.getRange().getKeyTimestampFilterPattern();
    byte[] rowKey = CellUtil.cloneRow(kv);
    if (keyPattern.length() != rowKey.length) {
      log.error("Row Key:" + rowKey + ", Pattern: " + keyPattern);
      log.error("Timestamp Filter Pattern and Row Key length do not match. Don't know how to handle this.");
      return 0;
    }
    byte[] ts = new byte[4];
    int j = 0;
    for (int i = keyPattern.indexOf("1"); i <= keyPattern.lastIndexOf("1"); i++, j++) {
      ts[j] = rowKey[i];
    }
    return Bytes.toInt(ts);
  }

  private long getTimestampFromRowKeyAsMillis(Cell kv, TimeseriesAggregateRequest request) {
    long ts = getTimestampFromRowKeyAsSeconds(kv, request);
    return ts * 1000l;
  }

  private long getMaxTimeStamp(Scan scan, TimeseriesAggregateRequest request) {
    if (request.hasRange()) {
      long timestamp = request.getRange().getKeyTimestampMax();
      return timestamp * 1000l;
    }
    return scan.getTimeRange().getMax();
  }

  @Override
  public void getMax(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    InternalScanner scanner = null;
    TimeseriesAggregateResponse response = null;
    T max = null;
    boolean hasScannerRange = false;
    Map<Long, T> maximums = new HashMap<Long, T>();

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      List<TimeRange> timeRanges = getAllTimeRanges(scan, request);
      List<Cell> results = new ArrayList<Cell>();
      byte[] colFamily = scan.getFamilies()[0];
      boolean hasMoreRows = false;
      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          // if (intervalRange.getMin() < maxTimeStamp) {
          long timestamp = 0;
          if (hasScannerRange) timestamp = kv.getTimestamp();
          else timestamp =
              getMillisTimestampFromOffset(getTimestampFromRowKeyAsMillis(kv, request),
                Bytes.toInt(kv.getQualifier()));
          for (TimeRange t : timeRanges) {
            if (t.withinTimeRange(timestamp)) {
              long minTimestamp = t.getMin();
              if (maximums.containsKey(minTimestamp)) {
                max = maximums.get(minTimestamp);
              } else max = null;
              temp = ci.getValue(colFamily, kv.getQualifier(), kv);
              max = (max == null || (temp != null && ci.compare(temp, max) > 0)) ? temp : max;
              maximums.put(minTimestamp, max);
            }
          }
        }
      } while (hasMoreRows);
      if (!maximums.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, T> entry : maximums.entrySet()) {
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          valueBuilder.addFirstPart(ci.getProtoForCellType(entry.getValue()).toByteString());

          mapElementBuilder.setKey(entry.getKey());
          mapElementBuilder.setValue(valueBuilder.build());

          responseBuilder.addEntry(mapElementBuilder.build());
        }
        response = responseBuilder.build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {
        }
      }
    }
    log.info("Maximums from this region are " + env.getRegion().getRegionNameAsString() + ": "
        + maximums.toString());
    done.run(response);
  }

  @SuppressWarnings("unchecked")
  ColumnInterpreter<T, S, P, Q, R> constructColumnInterpreterFromRequest(
      TimeseriesAggregateRequest request) throws IOException {
    String className = request.getInterpreterClassName();
    Class<?> cls;
    try {
      cls = Class.forName(className);
      ColumnInterpreter<T, S, P, Q, R> ci = (ColumnInterpreter<T, S, P, Q, R>) cls.newInstance();
      if (request.hasInterpreterSpecificBytes()) {
        ByteString b = request.getInterpreterSpecificBytes();
        P initMsg = ProtobufUtil.getParsedGenericInstance(ci.getClass(), 2, b);
        ci.initialize(initMsg);
      }
      return ci;
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void getMin(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    InternalScanner scanner = null;
    TimeseriesAggregateResponse response = null;
    // TimeRange intervalRange = null;
    T min = null;
    boolean hasScannerRange = false;
    Map<Long, T> minimums = new HashMap<Long, T>();
    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      List<TimeRange> timeRanges = getAllTimeRanges(scan, request);
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      byte[] colFamily = scan.getFamilies()[0];

      boolean hasMoreRows = false;
      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          long timestamp = 0;
          if (hasScannerRange) timestamp = kv.getTimestamp();
          else timestamp =
              getMillisTimestampFromOffset(getTimestampFromRowKeyAsMillis(kv, request),
                Bytes.toInt(kv.getQualifier()));
          for (TimeRange t : timeRanges) {
            if (t.withinTimeRange(timestamp)) {
              long minTimestamp = t.getMin();
              if (minimums.containsKey(minTimestamp)) {
                min = minimums.get(minTimestamp);
              } else min = null;
              temp = ci.getValue(colFamily, kv.getQualifier(), kv);
              min = (min == null || (temp != null && ci.compare(temp, min) < 0)) ? temp : min;
              minimums.put(minTimestamp, min);
            }
          }
        }
      } while (hasMoreRows);
      if (!minimums.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, T> entry : minimums.entrySet()) {
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          valueBuilder.addFirstPart(ci.getProtoForCellType(entry.getValue()).toByteString());

          mapElementBuilder.setKey(entry.getKey());
          mapElementBuilder.setValue(valueBuilder.build());

          responseBuilder.addEntry(mapElementBuilder.build());
        }
        response = responseBuilder.build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {
        }
      }
    }
    log.info("Minimums from this region are " + env.getRegion().getRegionNameAsString() + ": "
        + minimums.toString());
    done.run(response);
  }

  @Override
  public void getSum(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    TimeseriesAggregateResponse response = null;
    InternalScanner scanner = null;
    Map<Long, S> sums = new HashMap<Long, S>();
    boolean hasScannerRange = false;

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null;
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      List<TimeRange> timeRanges = getAllTimeRanges(scan, request);
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      List<Cell> results = new ArrayList<Cell>();
      boolean hasMoreRows = false;
      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          long timestamp = 0;
          if (hasScannerRange) timestamp = kv.getTimestamp();
          else timestamp =
              getMillisTimestampFromOffset(getTimestampFromRowKeyAsMillis(kv, request),
                Bytes.toInt(kv.getQualifier()));
          for (TimeRange t : timeRanges) {
            if (t.withinTimeRange(timestamp)) {
              long minTimestamp = t.getMin();
              if (sums.containsKey(minTimestamp)) {
                sumVal = sums.get(minTimestamp);
              } else sumVal = null;
              temp = ci.getValue(colFamily, kv.getQualifier(), kv);
              if (temp != null) sumVal = ci.add(sumVal, ci.castToReturnType(temp));
              sums.put(minTimestamp, sumVal);
            }
          }
        }
      } while (hasMoreRows);
      if (!sums.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, S> entry : sums.entrySet()) {
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();
          valueBuilder.addFirstPart(ci.getProtoForPromotedType(entry.getValue()).toByteString());
          mapElementBuilder.setKey(entry.getKey());
          mapElementBuilder.setValue(valueBuilder.build());
          responseBuilder.addEntry(mapElementBuilder.build());
        }
        response = responseBuilder.build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {
        }
      }
    }
    log.info("Sums from this region are " + env.getRegion().getRegionNameAsString() + ": "
        + sums.toString());
    done.run(response);
  }

  @Override
  public void getAvg(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    TimeseriesAggregateResponse response = null;
    InternalScanner scanner = null;
    Map<Long, SimpleEntry<Long, S>> averages = new HashMap<Long, SimpleEntry<Long, S>>();
    boolean hasScannerRange = false;

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null;
      T temp;
      Long kvCountVal = 0l;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      List<TimeRange> timeRanges = getAllTimeRanges(scan, request);
      byte[] colFamily = scan.getFamilies()[0];

      List<Cell> results = new ArrayList<Cell>();
      boolean hasMoreRows = false;

      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          long timestamp = 0;
          if (hasScannerRange) timestamp = kv.getTimestamp();
          else timestamp =
              getMillisTimestampFromOffset(getTimestampFromRowKeyAsMillis(kv, request),
                Bytes.toInt(kv.getQualifier()));
          for (TimeRange t : timeRanges) {
            if (t.withinTimeRange(timestamp)) {
              long minTimestamp = t.getMin();
              if (averages.containsKey(minTimestamp)) {
                sumVal = averages.get(minTimestamp).getValue();
                kvCountVal = averages.get(minTimestamp).getKey();
              } else {
                sumVal = null;
                kvCountVal = 0l;
              }
              temp = ci.getValue(colFamily, kv.getQualifier(), kv);
              if (temp != null) {
                kvCountVal++;
                sumVal = ci.add(sumVal, ci.castToReturnType(temp));
                averages.put(t.getMin(), new AbstractMap.SimpleEntry<Long, S>(kvCountVal, sumVal));
              }
            }
          }
        }
      } while (hasMoreRows);
      if (!averages.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Entry<Long, SimpleEntry<Long, S>> entry : averages.entrySet()) {
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();
          ByteString first = ci.getProtoForPromotedType(entry.getValue().getValue()).toByteString();
          valueBuilder.addFirstPart(first);
          ByteBuffer bb = ByteBuffer.allocate(8).putLong(entry.getValue().getKey());
          bb.rewind();
          valueBuilder.setSecondPart(ByteString.copyFrom(bb));
          mapElementBuilder.setKey(entry.getKey());
          mapElementBuilder.setValue(valueBuilder.build());
          responseBuilder.addEntry(mapElementBuilder.build());
        }
        response = responseBuilder.build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {
        }
      }
    }
    log.info("Averages from this region are " + env.getRegion().getRegionNameAsString() + ": "
        + averages.toString());
    done.run(response);
  }
}