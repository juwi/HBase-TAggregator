package org.apache.hadoop.hbase.common.aggregation;

import com.google.protobuf.Message;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by juwi on 11/26/16.
 */
public class Sum<T, S, P extends Message, Q extends Message, R extends Message> implements Aggregator {


    @Override
    public <T, S, P extends Message, Q extends Message, R extends Message> Map<Long, S> compute(
            Map results, Cell kv, ColumnInterpreter<T, S, P, Q, R> ci, byte[] columnFamily, long timestamp,
            List<TimeRange> timeRanges) throws IOException {
        Map<Long, S> sums = results;
        ColumnInterpreter<T, S, P, Q, R> columnInterpreter = ci;
        T temp;
        S sum;
        for (TimeRange t : timeRanges) {
            if (t.withinTimeRange(timestamp)) {
                long minTimestamp = t.getMin();
                if (sums.containsKey(minTimestamp)) {
                    sum = sums.get(minTimestamp);
                } else sum = null;
                temp = ci.getValue(columnFamily, CellUtil.cloneQualifier(kv), kv);
                if (temp != null) sum = ci.add(sum, ci.castToReturnType(temp));
                sums.put(minTimestamp, sum);
            }
        }
        return sums;
    }

    @Override
    public TimeseriesAggregateProtos.TimeseriesAggregateResponse wrapForTransport(Map results, ColumnInterpreter ci) {
        Map<Long, S> sums = results;
        TimeseriesAggregateProtos.TimeseriesAggregateResponse.Builder responseBuilder =
                TimeseriesAggregateProtos.TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, S> entry : sums.entrySet()) {
            TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry.Builder valueBuilder =
                    TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry.newBuilder();
            TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
                    TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry.newBuilder();
            valueBuilder.addFirstPart(ci.getProtoForPromotedType(entry.getValue()).toByteString());
            mapElementBuilder.setKey(entry.getKey());
            mapElementBuilder.setValue(valueBuilder.build());
            responseBuilder.addEntry(mapElementBuilder.build());
        }
        return responseBuilder.build();
    }
}
