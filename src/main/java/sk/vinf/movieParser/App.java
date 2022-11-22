package sk.vinf.movieParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import java.util.ArrayList;
import java.util.random.RandomGenerator.ArbitrarilyJumpableGenerator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Iterator;

import org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;

import org.semanticweb.yars.nx.parser.NxParser;

import scala.collection.Seq;

import org.semanticweb.yars.nx.Node;

public class App {
    public static String getObjectId(Node[] ns) {
        Pattern idPattern = Pattern.compile("<http://rdf.freebase.com/ns/(?<id>.+)>");
        Matcher matcherId = idPattern.matcher(ns[0].toString());
        if (matcherId.find()) {
            return matcherId.group("id");
        } else {
            return "null";
        }
    }

    public static NxParser loadFile(String fileName) {
        try {
            FileInputStream file = new FileInputStream(fileName);
            NxParser nxp = new NxParser();
            nxp.parse(file);

            return nxp;
        } catch (Exception e) {
            System.out.println("Cannot load the file");
        }

        return null;
    }

    public static String findFilmId(Node[] ns, String filmId, JSONObject films) {
        Pattern idPattern = Pattern.compile("<http://rdf.freebase.com/ns/(?<id>.\\.[a-zA-Z0-9]+)>");
        Pattern typePattern = Pattern.compile(".+type.object.type>");
        Pattern filmPattern = Pattern.compile(".+film.film>");
        Matcher matcherFilm = filmPattern.matcher(ns[2].toString());
        Matcher matcherId = idPattern.matcher(ns[0].toString());
        Matcher matcherType = typePattern.matcher(ns[1].toString());
        if (matcherId.find() && matcherType.find() && matcherFilm.find()) {
            try {
                films.put(matcherId.group("id"), new JSONObject());
                return matcherId.group("id");
            } catch (Exception e) {
                System.out.println("Canoot create JSON object");
                return matcherId.group("id");
            }
        } else {
            return filmId;
        }
    }

    public static JSONObject findFilmSpecs(String filmId, Node[] ns, JSONObject films) {
        Pattern titleTextPattern = Pattern.compile(".(?<title>.+).@en");
        Pattern idPattern = Pattern.compile("<http://rdf.freebase.com/ns/(?<id>.\\.[a-zA-Z0-9]+)>");
        Pattern titlePattern = Pattern.compile(".+type.object.name>");
        Pattern releasePattern = Pattern.compile(".+film.film.initial_release_date>");
        Pattern datePattern = Pattern.compile("\"(?<date>.+)\".+");
        Pattern descriptionPattern = Pattern.compile(".+common.topic.description>");
        Pattern writterPattern = Pattern.compile(".+written.+by (?<writtername>.+) (?<writtersurname>.+) and|.+");
        Pattern directorPattern = Pattern.compile(".+directed.+by (?<directorname>.+) (?<directorsurname>.+).{0,}");

        Matcher matcherId = idPattern.matcher(ns[0].toString());
        Matcher matcherTitle = titlePattern.matcher(ns[1].toString());
        Matcher matcherRelease = releasePattern.matcher(ns[1].toString());
        Matcher matcherTitleText = titleTextPattern.matcher(ns[2].toString());
        Matcher matcherDate = datePattern.matcher(ns[2].toString());
        Matcher matcherDescription = descriptionPattern.matcher(ns[1].toString());
        Matcher matcherWritter = writterPattern.matcher(ns[2].toString());
        Matcher matcherDirector = directorPattern.matcher(ns[2].toString());

        if (matcherId.find() && filmId.equalsIgnoreCase(matcherId.group("id"))) {
            JSONObject film = null;
            try {
                film = films.getJSONObject(filmId);
            } catch (Exception e) {
                System.out.println("cannot put specs");
            }

            if (matcherTitle.find() && matcherTitleText.find()) {
                film.put("title", matcherTitleText.group("title"));
                /// System.out.println(matcherTitleText.group("title"));
            } else if (matcherRelease.find() && matcherDate.find()) {
                film.put("release_year", matcherDate.group("date"));
                // System.out.println(ns[2].toString());
            } else if (matcherDescription.find()) {
                // System.out.println(filmId);
                // System.out.println(ns[2].toString());
                if (matcherWritter.find()) {
                    film.put("writter", matcherWritter.group("writtername") + matcherWritter.group("writtersurname"));

                }
                if (matcherDirector.find()) {
                    film.put("director",
                            matcherDirector.group("directorname") + matcherDirector.group("directorsurname"));

                }
                // System.out.println(ns[2].toString());
            }
            films.put(filmId, film);

            return films;
        }

        return films;
    }

    public static Boolean isFilm(JSONObject object) {
        try {
            JSONArray objectTypes = object.getJSONArray("<http://rdf.freebase.com/ns/type.object.type>");
            for (int i = 0; i < objectTypes.length(); i++) {
                if (objectTypes.get(i).toString().equalsIgnoreCase("<http://rdf.freebase.com/ns/film.film>")) {
                    // System.out.println(object);
                    return true;
                }
            }

        } catch (Exception e) {
            // System.out.println("Object has no object.type predicate");
        }
        return false;
    }

    public static JSONObject getFilmTitle(JSONObject film, JSONObject object) {
        JSONArray rawTitles;
        JSONArray titles = new JSONArray();
        Pattern titleTextPattern = Pattern.compile(".(?<title>.+).@.+");
        try {
            rawTitles = object.getJSONArray("<http://rdf.freebase.com/ns/type.object.name>");
        } catch (Exception e) {
            // System.out.println("Film do not have title");
            return film;
        }
        // Pattern titleTextPattern = Pattern.compile(".(?<title>.+).@en");
        // Matcher matcherTitleText = titleTextPattern.matcher(ns[2].toString());
        for (int i = 0; i < rawTitles.length(); i++) {
            Matcher matcherTitleText = titleTextPattern.matcher(rawTitles.getString(i));
            if (matcherTitleText.find()) {
                titles.put(matcherTitleText.group("title"));
            }
        }
        film.put("title", titles);

        return film;
    }

    public static JSONObject getReleaseYear(JSONObject film, JSONObject object) {
        JSONArray years;
        Pattern datePattern = Pattern.compile("\"(?<date>.+)\".+");
        try {
            years = object.getJSONArray("<http://rdf.freebase.com/ns/film.film.initial_release_date>");
        } catch (Exception e) {
            return film;
        }
        Matcher matcherDate = datePattern.matcher(years.getString(0));
        if (matcherDate.find()) {
            film.put("release year", matcherDate.group("date"));
        } else {
            film.put("release year", years.getString(0));
        }
        return film;
    }

    public static JSONObject getFilmDirector(JSONObject film, JSONObject object, JSONObject objects) {
        String director = new String();
        try {
            director = object.getJSONArray("<http://rdf.freebase.com/ns/film.film.directed_by>").getString(0);
        } catch (Exception e) {
            director = null;
        }
        if (director != null) {
            Pattern directorPattern = Pattern.compile(".(?<name>.+).@.+");
            try {
                JSONObject person = objects.getJSONObject(director);
                String personName = person.getJSONArray("<http://rdf.freebase.com/ns/type.object.name>").getString(0);
                Matcher matcherDirector = directorPattern.matcher(personName);
                if (matcherDirector.find()) {
                    film.put("Directed by", matcherDirector.group("name"));
                } else {
                    film.put("Directed by", "None");
                }

            } catch (Exception e) {
                film.put("Directed by", director);
            }
        } else {
            film.put("Directed by", "None");
        }

        return film;
    }

    public static JSONObject getFilmWritter(JSONObject film, JSONObject object, JSONObject objects) {
        String writter = new String();
        try {
            writter = object.getJSONArray("<http://rdf.freebase.com/ns/film.film.written_by>").getString(0);
        } catch (Exception e) {
            writter = null;
        }
        if (writter != null) {
            Pattern writterPattern = Pattern.compile(".(?<name>.+).@.+");
            try {
                JSONObject person = objects.getJSONObject(writter);
                String personName = person.getJSONArray("<http://rdf.freebase.com/ns/type.object.name>").getString(0);
                Matcher matcherWritter = writterPattern.matcher(personName);
                if (matcherWritter.find()) {
                    film.put("Written by", matcherWritter.group("name"));
                } else {
                    film.put("Written by", "None");
                }

            } catch (Exception e) {
                film.put("Written by", writter);
            }
        } else {
            film.put("Written by", "None");
        }

        return film;
    }

    public static JSONObject getFilmCountry(JSONObject film, JSONObject object, JSONObject objects) {
        String country = new String();
        try {
            country = object.getJSONArray("<http://rdf.freebase.com/ns/film.film.country>").getString(0);
        } catch (Exception e) {
            country = null;
        }
        if (country != null) {
            Pattern countryPattern = Pattern.compile(".(?<name>.+).@.+");
            try {
                JSONObject coun = objects.getJSONObject(country);
                String countryName = coun.getJSONArray("<http://rdf.freebase.com/ns/type.object.name>").getString(0);
                Matcher matcherCountry = countryPattern.matcher(countryName);
                if (matcherCountry.find()) {
                    film.put("Country", matcherCountry.group("name"));
                } else {
                    film.put("Country", "None");
                }

            } catch (Exception e) {
                film.put("Country", country);
            }
        } else {
            film.put("Country", "None");
        }
        return film;
    }

    public static JSONObject getFilmGenre(JSONObject film, JSONObject object, JSONObject objects) {
        String genre = new String();
        try {
            genre = object.getJSONArray("<http://rdf.freebase.com/ns/film.film.genre>").getString(0);
        } catch (Exception e) {
            genre = null;
        }
        if (genre != null) {
            Pattern genrePattern = Pattern.compile(".(?<name>.+).@.+");
            try {
                JSONObject gen = objects.getJSONObject(genre);
                String genreName = gen.getJSONArray("<http://rdf.freebase.com/ns/type.object.name>").getString(0);
                Matcher matcherGenre = genrePattern.matcher(genreName);
                if (matcherGenre.find()) {
                    film.put("Genre", matcherGenre.group("name"));
                } else {
                    film.put("Genre", "None");
                }

            } catch (Exception e) {
                film.put("Genre", genre);
            }
        } else {
            film.put("Genre", "None");
        }
        return film;
    }

    public static JSONObject getFilmSpecs(JSONObject films, String filmID, JSONObject objects) {
        JSONObject film = new JSONObject();
        JSONObject object = new JSONObject();
        try {
            object = objects.getJSONObject(filmID);
        } catch (Exception e) {
            System.out.println("Can not find object" + filmID);
        }
        film = getFilmTitle(film, object);
        film = getReleaseYear(film, object);
        film = getFilmDirector(film, object, objects);
        film = getFilmWritter(film, object, objects);
        film = getFilmCountry(film, object, objects);
        film = getFilmGenre(film, object, objects);

        try {
            film.getJSONArray("title");
        } catch (Exception e) {
            return films;
        }
        films.put(filmID, film);
        // objects.remove(filmID);
        return films;
    }

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("App").setMaster("spark://localhost:7077").setJars(new String[]{"jars/App.jar"});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        long start = System.currentTimeMillis();

        String path = "/Users/rudy/Documents/FIIT/VINF/Projekt/Data/freebase-head-1000000";
        StructType csvSchema = new StructType()
            .add("subject", "string")
            .add("predicate", "string")
            .add("object", "string")
            .add("dot", "string");

        StructType schema = new StructType()
            .add("subject", "string")
            .add("predicate", "string");
        
        Dataset<Row>  lines = sparkSession.read().option("delimiter", "\t").schema(csvSchema).csv(path);
        lines = lines.drop("dot");

        Dataset<Row>  films = lines.filter(lines.col("object").equalTo("<http://rdf.freebase.com/ns/film.film>"));
        films = films.dropDuplicates("subject");
        
        Dataset<Row>  titles = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/type.object.name>")).select("subject", "object").withColumnRenamed("object", "title");
        titles = titles.withColumnRenamed("subject", "subold");
        titles = titles.dropDuplicates("subold");
        
        Dataset<Row>  directors = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/film.film.directed_by>")).withColumnRenamed("object", "directorId");
        directors = directors.withColumnRenamed("subject", "subold");

        Dataset<Row>  writters = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/film.film.written_by>")).withColumnRenamed("object", "writterId");
        writters = writters.withColumnRenamed("subject", "subold");


        Dataset<Row> filmRelease = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/film.film.initial_release_date>")).select("subject", "object");
        filmRelease = filmRelease.withColumnRenamed("object", "year");
        filmRelease = filmRelease.withColumnRenamed("subject", "subold");

        // System.out.println(films.count());
        films = films.join(titles, films.col("subject").equalTo(titles.col("subold")), "inner").select("subject", "title").withColumnRenamed("title", "filmTitle");
        films = films.join(filmRelease, films.col("subject").equalTo(filmRelease.col("subold")), "left").select("subject", "filmTitle", "year");
        films = films.join(directors, films.col("subject").equalTo(directors.col("subold")), "left").select("subject", "filmTitle", "year", "directorId");
        films = films.join(writters, films.col("subject").equalTo(writters.col("subold")), "left").select("subject", "filmTitle", "year", "directorId", "writterId");
        films = films.join(titles, films.col("directorId").equalTo(titles.col("subold")), "left").select("subject", "filmTitle", "year", "directorId", "writterId", "title").withColumnRenamed("title", "directorTitle");
        
        Pattern p = Pattern.compile("\"([^\"]*)\"");
        JavaRDD<Row> filmy = films.toJavaRDD();

        // filmy = filmy.map( row -> {
        //     Matcher m = p.matcher(row.getAs("filmTitle"));
        //     return RowFactory.create(row.getAs("subject"), m.group(1));
        //   });
        // films.show(5);

        films = films.rdd().map((MapFunction<String, String>) row -> {
            Matcher m = p.matcher(row);
            return m.group(1);
        }, Encoders.STRING());

        
        // films = sparkSession.createDataset(films.toJavaRDD().map(row -> {
        //     Matcher m = p.matcher(row.getAs("filmTitle"));
        //     return RowFactory.create(row.getAs("subject"), m.group(1));
        //   }).rdd(), Encoders.bean(Row.class));
        
        // films.select("") = films.map(new MapFunction<Row,Row>(){
        //     @Override
        //     public Row call(Row row) throws Exception {
        //         Row newRow = RowFactory.create(row.getAs("subject"));
        //         return newRow;
        //     }
        // }, Encoders.bean(Row.class));
        films.show(5);
        // System.out.println(films.showString(18, 0, true));

        long end = System.currentTimeMillis();
        float sec = (end - start) / 1000F;
        System.out.println(sec + " seconds");



        // List<String> list = lines.as(Encoders.STRING()).collectAsList();
        // Dataset<String> df1 = sparkSession.createDataset(list, Encoders.STRING()); 
    
        // Dataset<String>  newRDD = df1.map((MapFunction<String, String>) k -> k.replaceAll(Pattern.quote("\\t."), ""), Encoders.STRING());

        // long start = System.currentTimeMillis();

        // Path path = Path.of("index.json");
        // String parsedContent = Files.readString(path);
        // JSONObject films = new JSONObject(parsedContent);

        // Indexer index = new Indexer();
        // index.buildIndex(films);
        // index.search("homme");

        // long end = System.currentTimeMillis();
        // float sec = (end - start) / 1000F;
        // System.out.println(sec + " seconds");
    }
}