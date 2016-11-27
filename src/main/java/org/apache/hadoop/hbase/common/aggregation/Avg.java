package org.apache.hadoop.hbase.common.aggregation;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by juwi on 11/27/16.
 */
public class Avg<T, S, P extends Message, Q extends Message, R extends Message> implements Aggregator {
    @Override
    public <T, S, P extends Message, Q extends Message, R extends Message> Map compute(
            Map results, Cell kv, ColumnInterpreter<T, S, P, Q, R> ci, byte[] columnFamily, long timestamp,
            List<TimeRange> timeRanges) throws IOException {
        Map<Long,Pair<Long, S>> avg = results;
        Count countAggregator = new Count();
        Sum sumAggregator = new Sum();
        Map<Long,Long> countVals = new HashMap<>();
        Map<Long, S> sumVals = new HashMap<>();
        for(Map.Entry<Long, Pair<Long, S>> e : avg.entrySet()) {
            countVals.put(e.getKey(), e.getValue().getFirst());
            sumVals.put(e.getKey(), e.getValue().getSecond());
        }
        Map<Long, S> sums = sumAggregator.compute(sumVals, kv, ci, columnFamily, timestamp, timeRanges);
        Map<Long, Long> counts = countAggregator.compute(countVals, kv, ci, columnFamily, timestamp, timeRanges);
        for(Map.Entry<Long, Long> e : counts.entrySet()) {
            avg.put(e.getKey(), new Pair(e.getValue(), sums.get(e.getKey())));
        }
        return avg;
    }

    @Override
    public TimeseriesAggregateProtos.TimeseriesAggregateResponse wrapForTransport(Map results, ColumnInterpreter ci) {
        Map<Long,Pair<Long,S>> avgs = results;
        TimeseriesAggregateProtos.TimeseriesAggregateResponse.Builder responseBuilder =
                TimeseriesAggregateProtos.TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, Pair<Long, S>> entry : avgs.entrySet()) {
            TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry.Builder valueBuilder =
                    TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry.newBuilder();
            TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
                    TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry.newBuilder();
            ByteString first = ci.getProtoForPromotedType(entry.getValue().getSecond()).toByteString();
            valueBuilder.addFirstPart(first);
            ByteBuffer bb = ByteBuffer.allocate(8).putLong(entry.getValue().getFirst());
            bb.rewind();
            valueBuilder.setSecondPart(ByteString.copyFrom(bb));
            mapElementBuilder.setKey(entry.getKey());
            mapElementBuilder.setValue(valueBuilder.build());
            responseBuilder.addEntry(mapElementBuilder.build());
        }
        return responseBuilder.build();
    }
}
