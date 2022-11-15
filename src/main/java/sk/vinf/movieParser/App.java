package sk.vinf.movieParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;
import java.util.random.RandomGenerator.ArbitrarilyJumpableGenerator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Iterator;

import org.apache.jena.atlas.json.JsonObject;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;

import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.Node;




public class App {
    public static String getObjectId(Node[] ns){
        Pattern idPattern = Pattern.compile("<http://rdf.freebase.com/ns/(?<id>.+)>");
        Matcher matcherId = idPattern.matcher(ns[0].toString());
        if(matcherId.find()){
            return matcherId.group("id");
        }
        else{
            return "null";
        }
    }
    
    public static NxParser loadFile(String fileName){
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

    public static String findFilmId(Node[] ns, String filmId, JSONObject films){
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
        }
        else{
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
        
        if(matcherId.find() && filmId.equalsIgnoreCase(matcherId.group("id"))){
            JSONObject film = null;
            try {
               film =  films.getJSONObject(filmId);
            } catch (Exception e) {
                System.out.println("cannot put specs");
            }

            if (matcherTitle.find() && matcherTitleText.find()) {
                film.put("title", matcherTitleText.group("title"));
                ///System.out.println(matcherTitleText.group("title"));
            }
            else if (matcherRelease.find() && matcherDate.find()) {
                film.put("release_year", matcherDate.group("date"));
                //System.out.println(ns[2].toString());
            }
            else if (matcherDescription.find()) {
                //System.out.println(filmId);
                //System.out.println(ns[2].toString());
                if (matcherWritter.find()) {
                    film.put("writter", matcherWritter.group("writtername")+matcherWritter.group("writtersurname") );

                }
                if (matcherDirector.find()) {
                    film.put("director", matcherDirector.group("directorname")+matcherDirector.group("directorsurname") );

                }
                //System.out.println(ns[2].toString());
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
                if(objectTypes.get(i).toString().equalsIgnoreCase("<http://rdf.freebase.com/ns/film.film>")){
                    //System.out.println(object);
                    return true;
                }
            }
            
        } catch (Exception e) {
            //System.out.println("Object has no object.type predicate");
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
            //System.out.println("Film do not have title");
            return film;
        }
        //Pattern titleTextPattern = Pattern.compile(".(?<title>.+).@en");
        //Matcher matcherTitleText = titleTextPattern.matcher(ns[2].toString());
        for (int i = 0; i < rawTitles.length(); i++) {
            Matcher matcherTitleText = titleTextPattern.matcher(rawTitles.getString(i));
            if(matcherTitleText.find()){
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
        if(matcherDate.find()){
            film.put("release year", matcherDate.group("date"));
        }
        else{
            film.put("release year", years.getString(0));
        }
        return film;
    }

    public static JSONObject getFilmSpecs(JSONObject films, String filmID, JSONObject objects) {
        JSONObject film = new JSONObject();
        JSONObject object = new JSONObject();
        try {
            object = objects.getJSONObject(filmID);
        } catch (Exception e) {
            System.out.println("Can not find object"+filmID);
        }
        film = getFilmTitle(film, object);
        film = getReleaseYear(film, object);
        
        try {
            film.getJSONArray("title");
        } catch (Exception e) {
            System.out.println("film without title");
            return films;
        }
        films.put(filmID, film);
        //objects.remove(filmID);
        return films;
    }

    public static void main(String[] args) throws IOException {
        
        // NxParser nxp = loadFile("/Users/rudy/Documents/FIIT/VINF/Projekt/Data/freebase-head-10000000");
        // System.out.println("file loaded");
        // String filmId = "null";
        // JSONObject films = new JSONObject();
        // JSONObject objects = new JSONObject();
        // String objectId;


        // long start = System.currentTimeMillis();
        // while (nxp.hasNext()) {
        //     Node[] ns = nxp.next();
            
        //     if (ns.length == 3){
        //         String predicate = ns[1].toString();
        //         String object = ns[2].toString();
        //         objectId = getObjectId(ns);

        //         JSONObject specs = new JSONObject();
        //         JSONArray jsonArray = new JSONArray();

        //         try {
        //             specs = objects.getJSONObject(objectId);
        //         } catch (Exception e) {
        //             objects.put(objectId, new JSONObject());
        //         }
        //         try {
        //             jsonArray = specs.getJSONArray(predicate);
        //         } catch (Exception e) {
        //             specs.put(predicate, jsonArray);
        //         }

        //         jsonArray.put(object);
        //         specs.put(predicate, jsonArray);
        //         objects.put(objectId, specs);
        //     }
        // }
        // long end = System.currentTimeMillis();
        // float sec = (end - start) / 1000F;
        // System.out.println(sec + " seconds");
        // for(int i = objects.names().length()-1; i>=0; i--){
        //     filmId = objects.names().getString(i);
        //     if(isFilm(objects.getJSONObject(filmId))){
        //         // go funkcie pridaj nakoniec podmienky pre neakceptovanie filmu z dalsieho cyklu tym eliminujeme dalsie prechadzanie filmov
        //         films = getFilmSpecs(films, filmId, objects);
        //     }
        // }
        // for(int i = films.names().length()-1; i>=0; i--){
        //     String key = films.names().getString(i);
        //     JSONObject value = films.getJSONObject(key);
        //     if(value.isEmpty()){
        //         films.remove(key);
        //         continue;
        //     }
        //     try {
        //         value.getJSONArray("title");
        //     } catch (Exception e) {
        //         System.out.println("film without title");
        //         films.remove(key);
        //     }       
        // }
        // Files.writeString(Paths.get("index.json"), films.toString(4));

        // end = System.currentTimeMillis();
        // sec = (end - start) / 1000F;
        // System.out.println(sec + " seconds");

        long start = System.currentTimeMillis();

        Path path = Path.of("index.json");
        String parsedContent = Files.readString(path);
        JSONObject films = new JSONObject(parsedContent);

        Indexer index = new Indexer();
        index.buildIndex(films);
        index.search("homme");

        long end = System.currentTimeMillis();
        float sec = (end - start) / 1000F;
        System.out.println(sec + " seconds");
    }
}