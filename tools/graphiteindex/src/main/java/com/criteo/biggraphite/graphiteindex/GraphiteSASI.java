package com.criteo.biggraphite.graphiteindex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.index.transactions.IndexTransaction.Type;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.notifications.MemtableSwitchedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphiteSASI
    implements Index, INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(GraphiteSASI.class);

    public static String makeIndexName(ColumnDefinition column, int generation)
    {
        return String.format("GraphiteSASI_%s_%d", column.name.toString(), generation);
    }

    /**
     * Called by {@link org.apache.cassandra.schema.IndexMetadata} using JVM reflection.
     */
    public static Map<String, String> validateOptions(Map<String, String> options, CFMetaData cfm)
    {
        List<String> errors = new ArrayList<>();
        if (!(cfm.partitioner instanceof Murmur3Partitioner)) {
            errors.add(cfm.partitioner.getClass().getSimpleName() + " is not supported");
        }

        // Target is not a custom option, it contains the keyspace/table/column and is always
        // provided.
        String targetColumn = options.get("target");
        if (targetColumn == null) {
            errors.add("Missing target column");
        } else {
            Pair<ColumnDefinition, IndexTarget.Type> target = TargetParser.parse(cfm, targetColumn);
            if (target == null) {
                errors.add("Failed to retrieve target column: " + targetColumn);
            } else if (target.left.isComplex()) {
                errors.add("Complex columns are not supported");
            } else if (target.left.isPartitionKey()) {
                errors.add("Partition key columns are not supported");
            }
        }

        if (errors.size() > 0) {
            throw new ConfigurationException(StringUtils.join(errors, "; "));
        }

        Set<String> validOptions = new HashSet<String>(
            Arrays.asList("target")
        );
        return options
            .entrySet()
            .stream()
            .filter(e -> !validOptions.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private final ColumnFamilyStore baseCfs;
    private final IndexMetadata config;
    private final ColumnDefinition column;

    public GraphiteSASI(ColumnFamilyStore baseCfs, IndexMetadata config)
    {
        this.baseCfs = baseCfs;
        this.config = config;

        // FIXME(d.forest): column type is assumed to be text
        this.column = TargetParser.parse(baseCfs.metadata, config).left;

        baseCfs.getTracker().subscribe(this);
    }

    @Override public IndexMetadata getIndexMetadata()
    {
        return config;
    }

    @Override public Callable<?> getInitializationTask()
    {
        return () -> {
            SortedSet<SSTableReader> toRebuild = new TreeSet<>(
                (a, b) -> Integer.compare(a.descriptor.generation, b.descriptor.generation)
            );
            for (SSTableReader sstable : baseCfs.getTracker().getView().liveSSTables()) {
                toRebuild.add(sstable);
            }

            CompactionManager.instance.submitIndexBuild(
                new GraphiteSASIBuilder(baseCfs, column, toRebuild)
            );

            return null;
        };
    }

    @Override public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    @Override public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
    }

    @Override public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    @Override public Callable<?> getBlockingFlushTask()
    {
        return null;
    }

    @Override public Callable<?> getInvalidateTask()
    {
        return getTruncateTask(FBUtilities.timestampMicros());
    }

    @Override public Callable<?> getTruncateTask(long truncatedAt)
    {
        return () -> {
            // TODO(d.forest): drop index
            logger.info("UNIMPLEMENTED - getTruncateTask");
            return null;
        };
    }

    @Override public boolean shouldBuildBlocking()
    {
        return true;
    }

    @Override public boolean dependsOn(ColumnDefinition column)
    {
        return this.column.compareTo(column) == 0;
    }

    @Override public boolean supportsExpression(ColumnDefinition column, Operator op)
    {
        // LIKE is the operator that makes the most sense here (pattern-based queries).
        return dependsOn(column) && (op == Operator.LIKE || op == Operator.LIKE_MATCHES);
    }

    @Override public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    @Override public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return filter.without(
            findIndexFilterExpression(filter).orElse(null)
        );
    }

    @Override public long getEstimatedResultRows()
    {
        // Taken from SASI, makes this index higher-priority for the column it supports.
        return Long.MIN_VALUE;
    }

    @Override public void validate(PartitionUpdate update)
        throws InvalidRequestException
    {
    }

    @Override public Indexer indexerFor(
        DecoratedKey key, PartitionColumns columns, int nowInSec, OpOrder.Group opGroup,
        IndexTransaction.Type transactionType
    )
    {
        // TODO(d.forest): fill in Indexer methods for in-band index updates.
        // TODO(d.forest): SASI also adjusts Cassandra's memtable on-heap memory usage
        //                 when inserting new values in their in-memory index here, this
        //                 seems like a good practice.
        return new Indexer() {
            @Override public void begin() {}
            @Override public void insertRow(Row row) {}
            @Override public void updateRow(Row oldRow, Row newRow) {}
            @Override public void removeRow(Row row) {}
            @Override public void partitionDelete(DeletionTime deletionTime) {}
            @Override public void rangeTombstone(RangeTombstone tombstone) {}
            @Override public void finish() {}
        };
    }

    @Override public BiFunction<PartitionIterator, ReadCommand, PartitionIterator>
        postProcessorFor(ReadCommand command)
    {
        return (partitionIterator, readCommand) -> partitionIterator;
    }

    @Override public Searcher searcherFor(ReadCommand command)
        throws InvalidRequestException
    {
        Optional<String> maybePattern = extractGraphitePattern(command.rowFilter());
        if (!maybePattern.isPresent()) {
            throw new InvalidRequestException("Query does not relate to this index");
        }

        // TODO(d.forest): implement searcher and result iterator
        return (controller) -> null;
    }

    @Override public void handleNotification(INotification notification, Object sender)
    {
        // TODO (stubbed from SASI)

        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification notice = (SSTableAddedNotification) notification;
            //index.update(Collections.<SSTableReader>emptyList(), Iterables.toList(notice.added));
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification notice = (SSTableListChangedNotification) notification;
            //index.update(notice.removed, notice.added);
        }
        else if (notification instanceof MemtableRenewedNotification)
        {
            //index.switchMemtable();
        }
        else if (notification instanceof MemtableSwitchedNotification)
        {
            //index.switchMemtable(((MemtableSwitchedNotification) notification).memtable);
        }
        else if (notification instanceof MemtableDiscardedNotification)
        {
            //index.discardMemtable(((MemtableDiscardedNotification) notification).memtable);
        }
    }

    private Optional<RowFilter.Expression> findIndexFilterExpression(RowFilter filter) {
        return filter
            .getExpressions()
            .stream()
            .filter(e -> dependsOn(e.column()))
            .findFirst();
    }

    private Optional<String> extractGraphitePattern(RowFilter filter) {
        return findIndexFilterExpression(filter)
            .map(RowFilter.Expression::getIndexValue)
            .map(UTF8Type.instance::compose);
    }
}
