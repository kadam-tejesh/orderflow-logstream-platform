package com.orderflow.search_indexing_service.service;

import com.orderflow.search_indexing_service.dto.LogEntryRequest;
import com.orderflow.search_indexing_service.model.LogSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class IndexingService {

    private static final int COMMIT_BATCH_SIZE = 100;

    private final IndexWriter indexWriter;
    private final AtomicInteger uncommittedCount = new AtomicInteger(0);

    public void indexLog(LogEntryRequest logEntry) throws Exception {
        Document document = new Document();

        document.add(new StringField(LogSchema.LEVEL, logEntry.getLevel(), Field.Store.YES));
        document.add(new StringField(LogSchema.SERVICE, logEntry.getService(), Field.Store.YES));

        document.add(new LongPoint(LogSchema.TIMESTAMP, logEntry.getTimestamp()));
        document.add(new StoredField(LogSchema.TIMESTAMP, logEntry.getTimestamp()));

        document.add(new IntPoint(LogSchema.RESPONSE_TIME, logEntry.getResponseTime()));
        document.add(new StoredField(LogSchema.RESPONSE_TIME, logEntry.getResponseTime()));

        document.add(new TextField(LogSchema.MESSAGE, logEntry.getMessage(), Field.Store.YES));

        indexWriter.addDocument(document);

        // Commit in batches instead of per-document — cuts fsync overhead
        // dramatically under high-volume ingestion.
        if (uncommittedCount.incrementAndGet() >= COMMIT_BATCH_SIZE) {
            commitBatch();
        }
    }

    // Safety net: flush any partially-filled batch every 2 seconds so logs
    // don't sit unsearchable for too long during low-traffic periods.
    @Scheduled(fixedRate = 2000)
    public void flushPendingBatch() {
        if (uncommittedCount.get() > 0) {
            commitBatch();
        }
    }

    private synchronized void commitBatch() {
        try {
            indexWriter.commit();
            int flushed = uncommittedCount.getAndSet(0);
            log.debug("Committed batch of {} log(s) to index", flushed);
        } catch (Exception e) {
            log.error("Failed to commit index batch: {}", e.getMessage());
        }
    }
}
