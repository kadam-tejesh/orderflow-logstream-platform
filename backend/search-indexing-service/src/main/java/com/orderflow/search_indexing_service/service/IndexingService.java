package com.orderflow.search_indexing_service.service;

import com.orderflow.search_indexing_service.dto.LogEntryRequest;
import com.orderflow.search_indexing_service.model.LogSchema;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
public class IndexingService {

    private final IndexWriter indexWriter;

    private static final int COMMIT_INTERVAL = 1000;
    private final AtomicInteger pendingDocuments = new AtomicInteger();
    private final Object commitLock = new Object();

    public void indexLog(LogEntryRequest logEntry) throws Exception {
        Document document = new Document();

        // Exact-match keyword fields
        document.add(new StringField(
                LogSchema.LEVEL,
                logEntry.getLevel(),
                Field.Store.YES
        ));

        document.add(new StringField(
                LogSchema.SERVICE,
                logEntry.getService(),
                Field.Store.YES
        ));

        // Point fields for range queries, plus stored copies
        document.add(new LongPoint(
                LogSchema.TIMESTAMP,
                logEntry.getTimestamp()
        ));

        document.add(new StoredField(
                LogSchema.TIMESTAMP,
                logEntry.getTimestamp()
        ));

        document.add(new IntPoint(
                LogSchema.RESPONSE_TIME,
                logEntry.getResponseTime()
        ));

        document.add(new StoredField(
                LogSchema.RESPONSE_TIME,
                logEntry.getResponseTime()
        ));

        // Full-text field
        document.add(new TextField(
                LogSchema.MESSAGE,
                logEntry.getMessage(),
                Field.Store.YES
        ));

        indexWriter.addDocument(document);
        indexWriter.commit();
    }

    public void indexLogs(List<LogEntryRequest> logEntries) throws Exception {
        for (LogEntryRequest logEntry : logEntries) {
            Document document = new Document();

            document.add(new StringField(
                    LogSchema.LEVEL,
                    logEntry.getLevel(),
                    Field.Store.YES
            ));

            document.add(new StringField(
                    LogSchema.SERVICE,
                    logEntry.getService(),
                    Field.Store.YES
            ));

            document.add(new LongPoint(
                    LogSchema.TIMESTAMP,
                    logEntry.getTimestamp()
            ));

            document.add(new StoredField(
                    LogSchema.TIMESTAMP,
                    logEntry.getTimestamp()
            ));

            document.add(new IntPoint(
                    LogSchema.RESPONSE_TIME,
                    logEntry.getResponseTime()
            ));

            document.add(new StoredField(
                    LogSchema.RESPONSE_TIME,
                    logEntry.getResponseTime()
            ));

            document.add(new TextField(
                    LogSchema.MESSAGE,
                    logEntry.getMessage(),
                    Field.Store.YES
            ));

            indexWriter.addDocument(document);
        }

        // Commit periodically instead of after every batch
        if (!logEntries.isEmpty()) {
            int pending = pendingDocuments.addAndGet(logEntries.size());

            if (pending >= COMMIT_INTERVAL) {
                synchronized (commitLock) {
                    if (pendingDocuments.get() >= COMMIT_INTERVAL) {
                        indexWriter.commit();
                        pendingDocuments.addAndGet(-COMMIT_INTERVAL);
                    }
                }
            }
        }
    }
}