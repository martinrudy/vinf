package sk.vinf.movieParser;

import java.io.IOException;
import java.util.Scanner;



public class App {

    public static void search(String query) {
        long start = System.currentTimeMillis();
        Cleanser cleanser = new Cleanser();
        try {
            cleanser.clean();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Indexer index = new Indexer();
        index.buildIndex(cleanser.films);
        index.search(query);
    
        long end = System.currentTimeMillis();
        float sec = (end - start) / 1000F;
        System.out.println(sec + " seconds");
    }
    public static void main(String[] args) throws IOException {
        int switcher = 0;
        String title = "null";
        String director = "null";
        String combination = "null";
        String query;
        while(true){
            System.out.println("=======================================================");
            System.out.println("===To start parsing Data press 1                    ===");
            System.out.println("===To search by film title and director name press 2===");
            System.out.println("===To search by film title press 3                  ===");
            System.out.println("===To search by director name press 4               ===");
            System.out.println("=======================================================");
            Scanner myObj = new Scanner(System.in);
            switcher = myObj.nextInt();
            switch (switcher) {
                case 1:
                    SparkParser parser = new SparkParser();
                    parser.parse("/Users/rudy/Documents/FIIT/VINF/Projekt/Data/freebase-head-100000000");
                    switcher = 0;
                    break;
                case 2:
                    System.out.println("Type film title to search");
                    Scanner s = new Scanner(System.in);
                    title = s.nextLine();
                    System.out.println("Type director name to search");
                    Scanner d = new Scanner(System.in);
                    director = d.nextLine();
                    combination = "td";
                    query = combination + ":" + title + ":" + director;
                    search(query);
                    switcher = 0;
                    break;
                case 3:
                    System.out.println("Type film title to search");
                    Scanner l = new Scanner(System.in);
                    title = l.nextLine();
                    combination = "t";
                    query = combination + ":" + title + ":" + director;
                    search(query);
                    switcher = 0;
                    break;
                case 4:
                    System.out.println("Type film director to search");
                    Scanner p = new Scanner(System.in);
                    director = p.nextLine();
                    combination = "d";
                    query = combination + ":" + title + ":" + director;
                    search(query);
                    switcher = 0;
                    break;
                default:
                    break;
            }
        }
                
    }
}