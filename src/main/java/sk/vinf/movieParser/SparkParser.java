package sk.vinf.movieParser;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;


public class SparkParser {


    public void parse(String path) {
        SparkConf sparkConf = new SparkConf().setAppName("App").setMaster("spark://localhost:7077").setJars(new String[]{"jars/App.jar"});
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        long start = System.currentTimeMillis();


        StructType csvSchema = new StructType()
            .add("subject", "string")
            .add("predicate", "string")
            .add("object", "string")
            .add("dot", "string");
        
        Dataset<Row>  lines = sparkSession.read().option("delimiter", "\t").schema(csvSchema).csv(path);
        lines = lines.drop("dot");

        Dataset<Row>  films = lines.filter(lines.col("object").equalTo("<http://rdf.freebase.com/ns/film.film>"));
        films = films.dropDuplicates("subject");
        
        Dataset<Row>  titles = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/type.object.name>")).select("subject", "object").withColumnRenamed("object", "title");
        titles = titles.withColumnRenamed("subject", "subold");
        titles = titles.dropDuplicates("subold");
        
        Dataset<Row>  directors = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/film.film.directed_by>")).withColumnRenamed("object", "directorId");
        directors = directors.withColumnRenamed("subject", "subold");
        directors = directors.dropDuplicates("subold");

        Dataset<Row>  writters = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/film.film.written_by>")).withColumnRenamed("object", "writterId");
        writters = writters.withColumnRenamed("subject", "subold");
        writters = writters.dropDuplicates("subold");

        Dataset<Row>  country = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/film.film.country>")).withColumnRenamed("object", "countryId");
        country = country.withColumnRenamed("subject", "subold");
        country = country.dropDuplicates("subold");

        Dataset<Row>  genre = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/film.film.genre>")).withColumnRenamed("object", "genreId");
        genre = genre.withColumnRenamed("subject", "subold");
        genre = genre.dropDuplicates("subold");

        Dataset<Row> filmRelease = lines.filter(lines.col("predicate").equalTo("<http://rdf.freebase.com/ns/film.film.initial_release_date>")).select("subject", "object");
        filmRelease = filmRelease.withColumnRenamed("object", "year");
        filmRelease = filmRelease.withColumnRenamed("subject", "subold");
        filmRelease = filmRelease.dropDuplicates("subold");

        // System.out.println(films.count());
        films = films.join(titles, films.col("subject").equalTo(titles.col("subold")), "inner").select("subject", "title").withColumnRenamed("title", "filmTitle");
        films = films.join(filmRelease, films.col("subject").equalTo(filmRelease.col("subold")), "left").select("subject", "filmTitle", "year");
        films = films.join(directors, films.col("subject").equalTo(directors.col("subold")), "left").select("subject", "filmTitle", "year", "directorId");
        films = films.join(writters, films.col("subject").equalTo(writters.col("subold")), "left").select("subject", "filmTitle", "year", "directorId", "writterId");
        films = films.join(country, films.col("subject").equalTo(country.col("subold")), "left").select("subject", "filmTitle", "year", "directorId", "writterId", "countryId");
        films = films.join(genre, films.col("subject").equalTo(genre.col("subold")), "left").select("subject", "filmTitle", "year", "directorId", "writterId", "countryId", "genreId");
        
        
        films = films.join(titles, films.col("directorId").equalTo(titles.col("subold")), "left").select("subject", "filmTitle", "year", "directorId", "writterId", "title", "countryId", "genreId").withColumnRenamed("title", "directorTitle");
        films = films.join(titles, films.col("writterId").equalTo(titles.col("subold")), "left").select("subject", "filmTitle", "year", "directorTitle", "title", "countryId", "genreId").withColumnRenamed("title", "writterTitle");
        films = films.join(titles, films.col("countryId").equalTo(titles.col("subold")), "left").select("subject", "filmTitle", "year", "directorTitle", "writterTitle", "title", "genreId").withColumnRenamed("title", "countryTitle");
        films = films.join(titles, films.col("genreId").equalTo(titles.col("subold")), "left").select("subject", "filmTitle", "year", "directorTitle", "writtertitle", "countryTitle", "title").withColumnRenamed("title", "genreTitle");

        films.write().json("data/films/");
        
        long end = System.currentTimeMillis();
        float sec = (end - start) / 1000F;
        System.out.println(sec + " seconds");
    }
}
