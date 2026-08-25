package com.orderflow.search_indexing_service.service;

import com.orderflow.search_indexing_service.dto.LogEntryRequest;
import com.orderflow.search_indexing_service.dto.SearchResultResponse;
import com.orderflow.search_indexing_service.model.LogSchema;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class SearchService {

    private static final int MAX_RESULTS = 100;

    private final IndexWriter indexWriter;
    private final Analyzer analyzer;

    /**
     * Supports queries like: level:ERROR AND service:billing-api
     */
    public SearchResultResponse search(String queryString) throws Exception {
        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            IndexSearcher searcher = new IndexSearcher(reader);

            QueryParser parser = new QueryParser(LogSchema.MESSAGE, analyzer);
            Query query = parser.parse(queryString);

            TopDocs topDocs = searcher.search(query, MAX_RESULTS);

            List<LogEntryRequest> results = new ArrayList<>();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = searcher.doc(scoreDoc.doc);
                results.add(toLogEntry(doc));
            }

            return new SearchResultResponse(topDocs.totalHits.value, results);
        }
    }

    private LogEntryRequest toLogEntry(Document doc) {
        LogEntryRequest entry = new LogEntryRequest();
        entry.setLevel(doc.get(LogSchema.LEVEL));
        entry.setService(doc.get(LogSchema.SERVICE));
        entry.setMessage(doc.get(LogSchema.MESSAGE));
        entry.setTimestamp(Long.valueOf(doc.get(LogSchema.TIMESTAMP)));
        entry.setResponseTime(Integer.valueOf(doc.get(LogSchema.RESPONSE_TIME)));
        return entry;
    }
}
