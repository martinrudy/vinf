package sk.vinf.movieParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;
import java.util.random.RandomGenerator.ArbitrarilyJumpableGenerator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Iterator;
import org.json.JSONArray;
import org.json.JSONObject;

import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.Node;



public class App {
    
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
    
    public static void main(String[] args) throws IOException {
        NxParser nxp = loadFile("/Users/rudy/Documents/FIIT/VINF/movie-parser/data/freebase-head-1000000");
        String filmId = "null";
        JSONObject films = new JSONObject();

        while (nxp.hasNext()) {
            Node[] ns = nxp.next();
            
            
            if (ns.length == 3){
                filmId = findFilmId(ns, filmId, films);
                films = findFilmSpecs(filmId, ns, films);
            }
        }
        for(int i = films.names().length()-1; i>=0; i--){
            String key = films.names().getString(i);
            JSONObject value = films.getJSONObject(key);
            if(value.isEmpty()){
                films.remove(key);
            }
                
        }
        Files.writeString(Paths.get("index.json"), films.toString(4));
    }
}