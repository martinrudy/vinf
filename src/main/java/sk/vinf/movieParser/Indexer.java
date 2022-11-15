package sk.vinf.movieParser;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Indexer {
    private final Directory memoryIndex = new ByteBuffersDirectory();
    private final StandardAnalyzer analyzer = new StandardAnalyzer();

    public void buildIndex(JSONObject films) {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);

        IndexWriter writer;
        try {
            writer = new IndexWriter(memoryIndex, indexWriterConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for(int i = films.names().length()-1; i>=0; i--){
            String key = films.names().getString(i);
            JSONObject value = films.getJSONObject(key);
            Document document = new Document();
            try {
                document.add(new TextField("title", value.getJSONArray("title").getString(0), Field.Store.YES));
                document.add(new StoredField("year", value.toString()));
            } catch (Exception e) {
                System.out.println("film without title");
                System.out.println(value);
            }

            try {
                writer.addDocument(document);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void search(String strQuery) {

        Query query;
        try {
            query = new QueryParser("title", analyzer).parse(strQuery);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        IndexReader indexReader;
        try {
            indexReader = DirectoryReader.open(memoryIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        IndexSearcher searcher = new IndexSearcher(indexReader);

        TopDocs topDocs;
        try {
            topDocs = searcher.search(query, 10);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<Document> documents = new ArrayList<>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            try {
                documents.add(searcher.doc(scoreDoc.doc));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println("Number of results: " + documents.size());

        for (Document doc: documents) {
            System.out.println();
            System.out.println("Title: " + doc.getField("title").stringValue());

            System.out.println("Release year: " + doc.getField("year").stringValue());
            // JSONArray names = new JSONArray(doc.getField("alternativeNames").stringValue());
            // for(Object name : names) {
            //     if(name instanceof String) {
            //         System.out.println(name);
            //     }
            // }
        }
    }
}