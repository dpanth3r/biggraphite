package com.criteo.biggraphite.graphiteindex;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.automaton.RegExp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.ControlledRealTimeReopenThread;

public class MetricsIndex implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MetricsIndex.class);

    private final String name;
    private Optional<Path> indexPath;
    private Directory directory;
    private IndexWriter writer;
    private SearcherManager searcherMgr;
    private ControlledRealTimeReopenThread<IndexSearcher> reopener;

    public MetricsIndex(String name)
        throws IOException
    {
        this(name, Optional.empty());
    }

    public MetricsIndex(String name, Optional<Path> indexPath)
        throws IOException
    {
        this.name = name;
        this.indexPath = indexPath;

        Directory directory;
        if (indexPath.isPresent()) {
            directory = FSDirectory.open(indexPath.get());

            // Adds a RAMDirectory cache, which helps with low-frequently updates and
            // high-frequency re-open, which is likely to be the case for Graphite metrics.
            // 8MB max segment size, 64MB max cached bytes.
            directory = new NRTCachingDirectory(directory, 8, 64);
        } else {
            directory = new RAMDirectory();
        }

        initialize(directory);
    }

    private void initialize(Directory directory)
        throws IOException
    {
        IndexWriterConfig config = new IndexWriterConfig();
        // XXX(d.forest): do we want a different setRAMBufferSizeMB?

        this.directory = directory;
        this.writer = new IndexWriter(directory, config);
        this.searcherMgr = new SearcherManager(writer, new SearcherFactory());

        // Enforce changes becoming visible to search within 10 to 30 seconds.
        this.reopener = new ControlledRealTimeReopenThread<>(this.writer, searcherMgr, 30, 10);
        this.reopener.start();
    }

    @Override
    public void close()
        throws IOException
    {
        reopener.close();
        searcherMgr.close();
        writer.close();
        directory.close();
    }

    public boolean isInMemory() {
        return !indexPath.isPresent();
    }

    // TODO(d.forest): make this a static “copyAndForceMerge” procedure and have two independant
    //                 instances for Memtable and OnDisk
    public void makePersistent(Path indexPath)
        throws IllegalStateException, IOException
    {
        if (!(directory instanceof RAMDirectory)) {
            throw new IllegalStateException("Attempting to make a non-volatile index persistent");
        }

        // FIXME(d.forest): race condition & exceptions if a search occurs during makePersistent?
        reopener.close();
        searcherMgr.close();

        // Force in-memory merge of all segments into one before copying to disk.
        // XXX(d.forest): is it actually necessary to force commit before merging?
        writer.commit();
        writer.forceMerge(1);
        writer.commit();
        writer.close();

        Directory onDiskDirectory = FSDirectory.open(indexPath);
        for (String file : this.directory.listAll()) {
            onDiskDirectory.copyFrom(this.directory, file, file, IOContext.DEFAULT);
        }

        this.indexPath = Optional.of(indexPath);
        initialize(onDiskDirectory);
    }

    public synchronized void forceCommit()
    {
        logger.debug("{} - Forcing commit and refreshing searchers", name);

        try {
            writer.commit();
            searcherMgr.maybeRefreshBlocking();
        } catch(IOException e) {
            logger.error("{} - Cannot commit or refresh searchers", name, e);
        }
    }

    public void insert(String path)
    {
        logger.debug("{} - Inserting '{}'", name, path);

        Document doc = MetricPath.toDocument(path);

        try {
            writer.addDocument(doc);
        } catch(IOException e) {
            logger.error("{} - Cannot insert metric in index", name, e);
        }
    }

    public List<String> search(String pattern)
    {
        BooleanQuery query = patternToQuery(pattern);
        logger.debug("{} - Searching for '{}', generated query: {}", name, pattern, query);

        ArrayList<String> results = new ArrayList<>();
        Collector collector = new MetricsIndexCollector(
            doc -> results.add(MetricPath.fromDocument(doc))
        );

        try {
            IndexSearcher searcher = searcherMgr.acquire();
            try {
                // TODO(d.forest): we should probably use TimeLimitingCollector
                searcher.search(query, collector);
            } catch(IOException e) {
                logger.error("{} - Cannot finish search query", name, e);
            } finally {
                searcherMgr.release(searcher);
            }
        } catch(IOException e) {
            logger.error("{} - Cannot acquire index searcher", name, e);
        }

        return results;
    }

    /**
     * Generates a Lucene query which matches the same metrics the given Graphite
     * globbing pattern would.
     */
    private BooleanQuery patternToQuery(String pattern)
    {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

        int length = MetricPath.iterateOnElements(
            pattern,
            (element, depth) -> {
                // Wildcard-only fields do not filter anything
                if (element == "*") {
                    return;
                }

                String termName = MetricPath.FIELD_PART_PREFIX + depth;
                Query query;
                if (element.indexOf('{') != -1 || element.indexOf('[') != -1) {
                    query = new RegexpQuery(
                        new Term(termName, graphiteToRegex(element)),
                        RegExp.NONE
                    );
                } else if (element.indexOf('*') != -1 || element.indexOf('?') != -1) {
                    query = new WildcardQuery(new Term(termName, element));
                } else {
                    query = new TermQuery(new Term(termName, element));
                }

                queryBuilder.add(query, BooleanClause.Occur.MUST);
            }
        );

        queryBuilder.add(
            IntPoint.newExactQuery(MetricPath.FIELD_LENGTH, length),
            BooleanClause.Occur.MUST
        );

        return queryBuilder.build();
    }

    /**
     * Naively transforms a Graphite globbing pattern into a regular expression.
     */
    private String graphiteToRegex(String query)
    {
        return query
            .replace('{', '(')
            .replace('}', ')')
            .replace(',', '|')
            .replace('?', '.')
            .replace("*", ".*?");
    }
}
