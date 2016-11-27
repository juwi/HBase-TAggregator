/**
 * Copyright 2014-2015 Julian Wissmann
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed
 * to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.common.AggregationBuilder;
import org.apache.hadoop.hbase.common.aggregation.*;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateService;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

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
 *
 * @param <T> Cell value data type
 * @param <S> Promoted data type
 * @param <P> PB message that is used to transport initializer specific bytes
 * @param <Q> PB message that is used to transport Cell (<T>) instance
 * @param <R> PB message that is used to transport Promoted (<S>) instance
 **/
@InterfaceAudience.Private
public class TimeseriesAggregateImplementation<T, S, P extends Message, Q extends Message, R extends Message>
        extends TimeseriesAggregateService implements CoprocessorService, Coprocessor {
    protected static final Log log = LogFactory.getLog(TimeseriesAggregateImplementation.class);
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
        // nothing to do

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

    private List<TimeRange> getAllTimeRanges(Scan scan, TimeseriesAggregateRequest request)
            throws IOException {
        List<TimeRange> timeRanges = new ArrayList<>();
        Long max = getMaxTimeStamp(scan, request);
        TimeRange next = getInitialTimeRange(request, scan);
        timeRanges.add(next);
        do {
            next = getNextTimeRange(next, request.getTimeIntervalSeconds());
            timeRanges.add(next);
        } while (next.getMax() <= max);
        return timeRanges;
    }

    private long getMaxTimeStamp(Scan scan, TimeseriesAggregateRequest request) {
        if (request.hasRange()) {
            long timestamp = request.getRange().getKeyTimestampMax();
            return timestamp * 1000l;
        }
        return scan.getTimeRange().getMax();
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
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void getMax(RpcController controller, TimeseriesAggregateRequest request,
                       RpcCallback<TimeseriesAggregateResponse> done) {
        InternalScanner scanner = null;
        TimeseriesAggregateResponse response = null;
        boolean hasScannerRange = false;
        if (!request.hasRange()) {
            hasScannerRange = true; // When no timerange is being passed in via
            // the request, it is
            // assumed, that the scanner is
            // timestamp-range bound
        }
        try {
            ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
            Scan scan = ProtobufUtil.toScan(request.getScan());
            scanner = env.getRegion().getScanner(scan);
            AggregationBuilder a = new AggregationBuilder().setColumnInterpreter(ci).isScanTimeRanged(hasScannerRange)
                    .setKeySchema(request.getRange().getKeyTimestampFilterPattern()).setInternalScanner(scanner)
                    .setTimeRanges(getAllTimeRanges(scan, request)).setColumnFamily(scan.getFamilies()[0]).setAggregator(new Max());
            response = a.aggregate().build();
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
        done.run(response);
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
        boolean hasScannerRange = false;
        if (!request.hasRange()) {
            hasScannerRange = true; // When no timerange is being passed in via
            // the request, it is
            // assumed, that the scanner is
            // timestamp-range bound
        }
        try {
            ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
            Scan scan = ProtobufUtil.toScan(request.getScan());
            scanner = env.getRegion().getScanner(scan);
            AggregationBuilder a = new AggregationBuilder().setColumnInterpreter(ci).isScanTimeRanged(hasScannerRange)
                    .setKeySchema(request.getRange().getKeyTimestampFilterPattern()).setInternalScanner(scanner)
                    .setTimeRanges(getAllTimeRanges(scan, request)).setColumnFamily(scan.getFamilies()[0]).setAggregator(new Min());
            response = a.aggregate().build();
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
        done.run(response);
    }

    @Override
    public void getSum(RpcController controller, TimeseriesAggregateRequest request,
                       RpcCallback<TimeseriesAggregateResponse> done) {
        TimeseriesAggregateResponse response = null;
        InternalScanner scanner = null;
        boolean hasScannerRange = false;

        if (!request.hasRange()) {
            hasScannerRange = true; // When no timerange is being passed in via
            // the request, it is
            // assumed, that the scanner is
            // timestamp-range bound
        }
        try {
            ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
            Scan scan = ProtobufUtil.toScan(request.getScan());
            scanner = env.getRegion().getScanner(scan);
            AggregationBuilder a = new AggregationBuilder().setColumnInterpreter(ci).isScanTimeRanged(hasScannerRange)
                    .setKeySchema(request.getRange().getKeyTimestampFilterPattern()).setInternalScanner(scanner)
                    .setTimeRanges(getAllTimeRanges(scan, request)).setColumnFamily(scan.getFamilies()[0]).setAggregator(new Sum());
            response = a.aggregate().build();
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
        done.run(response);
    }

    @Override
    public void getAvg(RpcController controller, TimeseriesAggregateRequest request,
                       RpcCallback<TimeseriesAggregateResponse> done) {
        TimeseriesAggregateResponse response = null;
        InternalScanner scanner = null;
        boolean hasScannerRange = false;

        if (!request.hasRange()) {
            hasScannerRange = true; // When no timerange is being passed in via
            // the request, it is
            // assumed, that the scanner is
            // timestamp-range bound
        }
        try {
            ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
            Scan scan = ProtobufUtil.toScan(request.getScan());
            scanner = env.getRegion().getScanner(scan);
            AggregationBuilder a = new AggregationBuilder().setColumnInterpreter(ci).isScanTimeRanged(hasScannerRange)
                    .setKeySchema(request.getRange().getKeyTimestampFilterPattern()).setInternalScanner(scanner)
                    .setTimeRanges(getAllTimeRanges(scan, request)).setColumnFamily(scan.getFamilies()[0]).setAggregator(new Avg());
            response = a.aggregate().build();
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
        done.run(response);
    }

    @Override
    public void getCount(RpcController controller, TimeseriesAggregateRequest request, RpcCallback<TimeseriesAggregateResponse> done) {
        TimeseriesAggregateResponse response = null;
        InternalScanner scanner = null;
        boolean hasScannerRange = false;

        if (!request.hasRange()) {
            hasScannerRange = true; // When no timerange is being passed in via
            // the request, it is
            // assumed, that the scanner is
            // timestamp-range bound
        }
        try {
            ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
            Scan scan = ProtobufUtil.toScan(request.getScan());
            scanner = env.getRegion().getScanner(scan);
            AggregationBuilder a = new AggregationBuilder().setColumnInterpreter(ci).isScanTimeRanged(hasScannerRange)
                    .setKeySchema(request.getRange().getKeyTimestampFilterPattern()).setInternalScanner(scanner)
                    .setTimeRanges(getAllTimeRanges(scan, request)).setColumnFamily(scan.getFamilies()[0]).setAggregator(new Count());
            response = a.aggregate().build();
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
        done.run(response);
    }
}