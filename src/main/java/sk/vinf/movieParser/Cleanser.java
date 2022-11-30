package sk.vinf.movieParser;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.json.JSONException;
import org.json.JSONObject;

public class Cleanser {
    public JSONObject films = new JSONObject();

    public String cleanId(JSONObject film) {
        Pattern idPattern = Pattern.compile("<http://rdf.freebase.com/ns/(?<id>.+)>");
        String id;
        try {
            id = film.get("subject").toString();
            Matcher matcherId = idPattern.matcher(id);
            if (matcherId.find()) {
                id = matcherId.group("id");
            } else {
                id = "null";
            }
            film.remove("subject");
        } catch (Exception e) {
            return "null";
        }
        
        return id;
    }
    public JSONObject cleanTitle(JSONObject film) {
        Pattern pattern = Pattern.compile("\"([^\"]*)\"");
        String filmTitle;
        try {
            filmTitle = film.get("filmTitle").toString();
            Matcher matcher = pattern.matcher(filmTitle);
            if (matcher.find()) {
                filmTitle = matcher.group(1);
            } else {
                filmTitle = "null";
            }
        } catch (Exception e) {
            return film;
        }
        
        film.put("filmTitle", filmTitle);
        return film;
    }
    public JSONObject cleanDirector(JSONObject film) {
        Pattern pattern = Pattern.compile("\"([^\"]*)\"");
        String director;
        try {
            director = film.get("directorTitle").toString();
            Matcher matcher = pattern.matcher(director);
            if (matcher.find()) {
                director = matcher.group(1);
            } else {
                director = "null";
            }
        } catch (Exception e) {
            return film;
        }
        
        film.put("directorTitle", director);
        return film;
    }

    public JSONObject cleanWritter(JSONObject film) {
        Pattern pattern = Pattern.compile("\"([^\"]*)\"");
        String director;
        try {
            director = film.get("writtertitle").toString();
            Matcher matcher = pattern.matcher(director);
            if (matcher.find()) {
                director = matcher.group(1);
            } else {
                director = "null";
            }
        } catch (Exception e) {
            return film;
        }
        
        film.put("writtertitle", director);
        return film;
    }
    public JSONObject cleanCountry(JSONObject film) {
        Pattern pattern = Pattern.compile("\"([^\"]*)\"");
        String country;
        try {
            country = film.get("countryTitle").toString();
            Matcher matcher = pattern.matcher(country);
            if (matcher.find()) {
                country = matcher.group(1);
            } else {
                country = "null";
            }
        } catch (Exception e) {
            return film;
        }
        
        film.put("countryTitle", country);
        return film;
    }
    public JSONObject cleanGenre(JSONObject film) {
        Pattern pattern = Pattern.compile("\"([^\"]*)\"");
        String genre;
        try {
            genre = film.get("genreTitle").toString();
            Matcher matcher = pattern.matcher(genre);
            if (matcher.find()) {
                genre = matcher.group(1);
            } else {
                genre = "null";
            }
        } catch (Exception e) {
            return film;
        }
        
        film.put("genreTitle", genre);
        return film;
    }

    public JSONObject cleanYear(JSONObject film) {
        Pattern pattern = Pattern.compile("\"(?<date>.+)\".+");
        String year;
        try {
            year = film.get("year").toString();
            Matcher matcher = pattern.matcher(year);
            if (matcher.find()) {
                year = matcher.group("date");
            } else {
                year = "null";
            }
        } catch (Exception e) {
            return film;
        }
        
        film.put("year", year);
        return film;
    }

    public void clean() throws IOException {
        Path dir = Paths.get("data/films");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.json")) {
            for (Path p : stream) {
                FileInputStream fis = new FileInputStream(p.toString());
                Scanner sc = new Scanner(fis);
                
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    JSONObject film = new JSONObject(line);
                    String filmId = cleanId(film);
                    film = cleanYear(film);
                    film = cleanTitle(film);
                    film = cleanDirector(film);
                    film = cleanWritter(film);
                    film = cleanCountry(film);
                    film = cleanGenre(film);
                    this.films.put(filmId, film);
                }
                sc.close();
            }
        }
        Files.writeString(Paths.get("index.json"), films.toString(4));
        // System.out.println(films.names().length());
    }
}
