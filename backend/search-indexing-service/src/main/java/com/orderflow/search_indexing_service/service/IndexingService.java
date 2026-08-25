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

@Service
@RequiredArgsConstructor
public class IndexingService {

    private final IndexWriter indexWriter;

    public void indexLog(LogEntryRequest logEntry) throws Exception {
        Document document = new Document();

        // Exact-match keyword fields
        document.add(new StringField(LogSchema.LEVEL, logEntry.getLevel(), Field.Store.YES));
        document.add(new StringField(LogSchema.SERVICE, logEntry.getService(), Field.Store.YES));

        // Point fields for range queries, plus stored copies so we can read them back
        document.add(new LongPoint(LogSchema.TIMESTAMP, logEntry.getTimestamp()));
        document.add(new StoredField(LogSchema.TIMESTAMP, logEntry.getTimestamp()));

        document.add(new IntPoint(LogSchema.RESPONSE_TIME, logEntry.getResponseTime()));
        document.add(new StoredField(LogSchema.RESPONSE_TIME, logEntry.getResponseTime()));

        // Full-text field
        document.add(new TextField(LogSchema.MESSAGE, logEntry.getMessage(), Field.Store.YES));

        indexWriter.addDocument(document);
        indexWriter.commit();
    }
}
