package com.orderflow.loginestion.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;

public class LuceneLogIndexer implements AutoCloseable {

    private final Directory directory;
    private final IndexWriter indexWriter;

    public LuceneLogIndexer(Path indexPath) throws IOException {
        this.directory = FSDirectory.open(indexPath);

        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        this.indexWriter = new IndexWriter(directory, config);
    }

    public void indexLog(
            String timestamp,
            String level,
            String service,
            String message) throws IOException {

        Document document = new Document();

        document.add(new StringField("timestamp", timestamp, StringField.Store.YES));
        document.add(new StringField("level", level, StringField.Store.YES));
        document.add(new StringField("service", service, StringField.Store.YES));
        document.add(new TextField("message", message, TextField.Store.YES));

        indexWriter.addDocument(document);
        indexWriter.commit();
    }

    public int getIndexedDocumentCount() throws IOException {
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            return reader.numDocs();
        }
    }

    @Override
    public void close() throws IOException {
        indexWriter.close();
        directory.close();
    }
}