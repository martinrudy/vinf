package sk.vinf.movieParser;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
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
                document.add(new TextField("title", value.get("filmTitle").toString(), Field.Store.YES));
            } catch (Exception e) {
                
            }
            try {
                document.add(new TextField("director", value.get("directorTitle").toString(), Field.Store.YES));
            } catch (Exception e) {
                
            }
            try {
                document.add(new StoredField("writter", value.get("writterTitle").toString()));
            } catch (Exception e) {
                
            }
            try {
                document.add(new StoredField("country", value.get("countryTitle").toString()));
            } catch (Exception e) {
                
            }
            try {
                document.add(new StoredField("genre", value.get("genreTitle").toString()));
            } catch (Exception e) {
                
            }
            try {
                document.add(new StoredField("year", value.get("year").toString()));
            } catch (Exception e) {
                
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
        String[] queryParsed = strQuery.split(":");
        String combination = queryParsed[0];
        String title = queryParsed[1];
        String director = queryParsed[2];

        Builder boolQueryBuilder = new BooleanQuery.Builder();

        Query queryTitle;
        Query queryDirector;
        if(combination.equals("td")){
            try {
                queryTitle = new QueryParser("title", analyzer).parse(title);
                queryDirector = new QueryParser("director", analyzer).parse(director);
                boolQueryBuilder
                    .add(queryTitle, Occur.MUST)
                    .add(queryDirector, Occur.MUST);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            
        } else if(combination.equals("t")){
            try {
                queryTitle = new QueryParser("title", analyzer).parse(title);
                boolQueryBuilder.add(queryTitle, Occur.MUST);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (combination.equals("d")){
            try {
                queryDirector = new QueryParser("director", analyzer).parse(director);
                boolQueryBuilder.add(queryDirector, Occur.MUST);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    

        IndexReader indexReader;
        try {
            indexReader = DirectoryReader.open(memoryIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        IndexSearcher searcher = new IndexSearcher(indexReader);

        TopDocs topDocs;
        BooleanQuery boolQuery = boolQueryBuilder.build();
        try {
            topDocs = searcher.search(boolQuery, 10);
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

            try {
                System.out.println("Release year: " + doc.getField("year").stringValue());

            } catch (Exception e) {
                // TODO: handle exception
            }
        }
    }
}