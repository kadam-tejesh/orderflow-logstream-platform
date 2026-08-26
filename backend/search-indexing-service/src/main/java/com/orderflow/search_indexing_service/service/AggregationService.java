package com.orderflow.search_indexing_service.service;

import com.orderflow.search_indexing_service.model.LogSchema;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

@Service
@RequiredArgsConstructor
public class AggregationService {

    private static final int MAX_DOCS_SCANNED = 50_000;
    private static final long ONE_MINUTE_MS = 60_000L;

    private final IndexWriter indexWriter;

    /**
     * Buckets matching log timestamps into per-minute counts over the last `rangeMinutes`.
     * This is a manual bucketing pass rather than Lucene's lucene-facet module — same
     * "count per minute" output, without the extra taxonomy-index setup that module needs.
     */
    public Map<Long, Long> countPerMinuteBucket(int rangeMinutes) throws Exception {
        long now = System.currentTimeMillis();
        long windowStart = now - (rangeMinutes * ONE_MINUTE_MS);

        Query rangeQuery = LongPoint.newRangeQuery(LogSchema.TIMESTAMP, windowStart, now);

        Map<Long, Long> buckets = new TreeMap<>();

        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(rangeQuery, MAX_DOCS_SCANNED);

            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = searcher.doc(scoreDoc.doc);
                long timestamp = Long.parseLong(doc.get(LogSchema.TIMESTAMP));
                long minuteBucket = (timestamp / ONE_MINUTE_MS) * ONE_MINUTE_MS;
                buckets.merge(minuteBucket, 1L, Long::sum);
            }
        }

        // Fill in empty minutes with 0 so the chart doesn't have gaps
        Map<Long, Long> filled = new LinkedHashMap<>();
        for (long bucket = (windowStart / ONE_MINUTE_MS) * ONE_MINUTE_MS; bucket <= now; bucket += ONE_MINUTE_MS) {
            filled.put(bucket, buckets.getOrDefault(bucket, 0L));
        }
        return filled;
    }
}
