package com.orderflow.search_indexing_service.config;

import com.orderflow.search_indexing_service.model.LogSchema;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class LuceneConfig {

    @Value("${lucene.index.path}")
    private String indexPath;

    // level and service are exact-match keywords: don't tokenize/lowercase them.
    // message stays full-text via StandardAnalyzer.
    @Bean
    public Analyzer analyzer() {
        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        fieldAnalyzers.put(LogSchema.LEVEL, new KeywordAnalyzer());
        fieldAnalyzers.put(LogSchema.SERVICE, new KeywordAnalyzer());
        return new PerFieldAnalyzerWrapper(new StandardAnalyzer(), fieldAnalyzers);
    }

    @Bean
    public Directory directory() throws Exception {
        return FSDirectory.open(Path.of(indexPath));
    }

    @Bean
    public IndexWriter indexWriter(Directory directory, Analyzer analyzer) throws Exception {
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        return new IndexWriter(directory, config);
    }
}
