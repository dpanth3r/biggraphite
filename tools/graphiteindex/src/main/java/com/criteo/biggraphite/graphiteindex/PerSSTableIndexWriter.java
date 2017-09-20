package com.criteo.biggraphite.graphiteindex;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.criteo.biggraphite.graphiteindex.MetricsIndex;

public class PerSSTableIndexWriter implements SSTableFlushObserver
{
    private static final Logger logger = LoggerFactory.getLogger(PerSSTableIndexWriter.class);

    // XXX(d.forest): find a better value?
    private static final int POOL_SIZE = 8;

    private static final ThreadPoolExecutor INDEX_FLUSHER_MEMTABLE;
    private static final ThreadPoolExecutor INDEX_FLUSHER_GENERAL;

    static {
        Function<String, ThreadPoolExecutor> makeThreadPool = (name) ->
            new JMXEnabledThreadPoolExecutor(
                POOL_SIZE,
                POOL_SIZE,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("GraphiteSASI-" + name),
                "internal"
            );

        INDEX_FLUSHER_GENERAL = makeThreadPool.apply("General");
        INDEX_FLUSHER_GENERAL.allowCoreThreadTimeOut(true);

        INDEX_FLUSHER_MEMTABLE = makeThreadPool.apply("Memtable");
        INDEX_FLUSHER_MEMTABLE.allowCoreThreadTimeOut(true);
    }

    private final Descriptor descriptor;
    private final OperationType operation;
    private final ColumnDefinition column;
    private final MetricsIndex index;

    private final int nowInSec;

    private DecoratedKey currentKey;
    private long currentPosition;

    public PerSSTableIndexWriter(
        Descriptor descriptor, OperationType operation, ColumnDefinition column,
        MetricsIndex index
    )
    {
        this.descriptor = descriptor;
        this.operation = operation;
        this.column = column;
        this.index = index;

        this.nowInSec = FBUtilities.nowInSeconds();
    }

    @Override public void begin()
    {
    }

    @Override public void startPartition(DecoratedKey key, long position)
    {
        this.currentKey = key;
        this.currentPosition = position;
    }

    @Override public void nextUnfilteredCluster(Unfiltered unfiltered)
    {
        if (!unfiltered.isRow()) {
            return;
        }

        Row row = (Row) unfiltered;
        ByteBuffer value = ColumnIndex.getValueOf(column, row, nowInSec);
        if (value == null) {
            return;
        }

        String path = UTF8Type.instance.compose(value);
        logger.error("Indexing {} at {}", path, currentPosition);

        index.insert(path, currentPosition);
    }

    @Override public void complete()
    {
        // TODO ?
    }
}
