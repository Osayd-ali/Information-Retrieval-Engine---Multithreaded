package csc435.app;

import java.lang.System;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class AppInterface {
    private ProcessingEngine engine;

    public AppInterface(ProcessingEngine engine) {
        this.engine = engine;
    }

    public void readCommands() {
        System.out.println("Enter commands ('index <path>', 'search <term1> <term2> ...', 'quit'):"); // TO-DO implement the read commands method
        Scanner sc = new Scanner(System.in);
        String command;
        
        while (true) {
            System.out.print("> ");
            
            // read from command line
            command = sc.nextLine();

            // if the command is quit, terminate the program       
            if ("quit".equals(command)) {
                System.out.println("Ending the program");
                break;
            }
            
            // if the command begins with index, index the files from the specified directory
            else if (command.startsWith("index ")) {
                String directory = command.substring(6);  // TO-DO parse command and call indexFolder on the processing engine
                System.out.println("Provided directory getting Indexed: " + directory);
                IndexResult resultOfIndex = engine.indexFiles(directory);
                // TO-DO print the execution time and the total number of bytes read
                System.out.println("Time taken to index: " + resultOfIndex.executionTime + " seconds");
                System.out.println("Total bytes read: " + resultOfIndex.totalBytesRead);
            }
            // if the command begins with search, search for files that matches the query
            else if (command.startsWith("search ")) {
                String searchTerms = command.substring(7); // TO-DO parse command and call search on the processing engine
                ArrayList<String> termsOfSearch = new ArrayList<>(Arrays.asList(searchTerms.split("\\s+")));
                for(String termsSearch : termsOfSearch){
                    System.out.println(termsSearch);
                }
                if (termsOfSearch.isEmpty()) {
                    System.out.println("Please provide search terms.");
                    continue;
                }
                SearchResult searchResult = engine.searchFiles(termsOfSearch);
                System.out.println("Search has been completed in " + searchResult.excutionTime + " seconds");
                System.out.println("Top results with highest Frequency:");
                if (searchResult.documentFrequencies.isEmpty()) {
                    System.out.println("No documents found for the search query.");
                } else { // TO-DO print the execution time and the top 10 search results
                    for (DocPathFreqPair pair : searchResult.documentFrequencies) {
                        System.out.println(pair.documentPath + ": " + pair.wordFrequency);
                    }
                }
            }
            else {

                System.out.println("unrecognized command!");
            }
        }
        sc.close();
    }
}
