package csc435.app;

import java.io.File;
import java.io.IOException;
import java.nio.charset.MalformedInputException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class IndexResult {
    public double executionTime;
    public long totalBytesRead;

    public IndexResult(double executionTime, long totalBytesRead) {
        this.executionTime = executionTime;
        this.totalBytesRead = totalBytesRead;
    }
}

class DocPathFreqPair {
    public String documentPath;
    public long wordFrequency;

    public DocPathFreqPair(String documentPath, long wordFrequency) {
        this.documentPath = documentPath;
        this.wordFrequency = wordFrequency;
    }
}

class SearchResult {
    public double excutionTime;
    public ArrayList<DocPathFreqPair> documentFrequencies;

    public SearchResult(double executionTime, ArrayList<DocPathFreqPair> documentFrequencies) {
        this.excutionTime = executionTime;
        this.documentFrequencies = documentFrequencies;
    }
}

public class ProcessingEngine {
    // keep a reference to the index store
    private IndexStore store;
    
    // the number of worker threads to use during indexing
    private int numWorkerThreads;
    private long bytesIndexed = 0;

    public ProcessingEngine(IndexStore store, int numWorkerThreads) {
        this.store = store;
        this.numWorkerThreads = numWorkerThreads;
    }

    public IndexResult indexFiles(String folderPath) {
        // TO-DO get the start time
        long startTime = System.currentTimeMillis();
        LinkedList<File> dirsToProcess = new LinkedList<>(); //Creating a queue for directories to process
	File rootDir = new File(folderPath);
        if (!rootDir.exists() || !rootDir.isDirectory()) {
            System.out.println("Invalid directory: " + folderPath);
            return new IndexResult(0, 0);
        }
        dirsToProcess.add(new File(folderPath));
        // TO-DO crawl the folder path and extract all file paths
        while (!dirsToProcess.isEmpty()) {
            File currentDir = dirsToProcess.poll();
            File[] filesInlist = currentDir.listFiles(); //Storing all the files we get through the folder path in a file array.
            if (filesInlist == null) {//When no files are found in list
                System.out.println("No files found or unable to access directory: " + folderPath);
                return new IndexResult(0, 0);
            }
	    int totalFiles = filesInlist.length;
            if (totalFiles < numWorkerThreads) {
                numWorkerThreads = totalFiles; // Adjust the number of threads to the available files
            }
            // TO-DO create the worker threads and give to each worker a subset of the documents that need to be indexed
            int filesPerThread = (int)Math.ceil((double)filesInlist.length/numWorkerThreads);
            Thread[] threads = new Thread[numWorkerThreads];
            int threadCount = 0;
	    for(int i=0; i<numWorkerThreads; i++){
                int start = i * filesPerThread;
                int end = Math.min(start + filesPerThread, filesInlist.length);
		if (start >= totalFiles) {
                    break; // No more files to process
                }
		
                File[] fileSubset = Arrays.copyOfRange(filesInlist, start, end); //creating a subset of documents that need to be indexed.
                //Creating a new thread for each subset of values
                threads[threadCount] = new Thread(() -> { //overriding the run() method through anonymous inner class or lambda expression.
                    try {
                        for (File file : fileSubset){
                            if (file.isFile()) { //Making sure the path being given is a file
                                if (file.getName().equals(".DS_Store")){
                                    System.out.println("Skipping hidden file: "+file.getPath());
                                    continue;
                                }
                                System.out.println("file being processed: " + file.getPath());
                                long fileSize = file.length();
                                // TO-DO increment the total number of read bytes
                                synchronized (this) { // keeping this synchronized for safety as bytesIndexed will be shared by multiple threads.
                                    bytesIndexed += fileSize; //Updating the total bytes indexed for all the files by adding up each file's size.
                                }
                                // TO-DO for each file put the document path in the index store and retrieve the document number
                                long docID = store.putDocument(file.getPath()); // Store is a reference of IndexStore, and we put the file path in documentMap hashmap to assign a unique doc number and we also retrieve that doc number.
                                System.out.println("Document number assigned to doc is: "+ docID);
                                HashMap<String, Long> wordFrequencies = getWordFrequencies(file.getPath());
                                store.updateIndex(docID, wordFrequencies); // TO-DO update the main index with the word frequencies for each document
                                System.out.println("Indexed file is: " + file.getPath() + " | Size: " + fileSize + " bytes");
                            } else if (file.isDirectory()){
                                dirsToProcess.add(file);
                            }
                        }
                    } catch (Exception e) { e.printStackTrace(); }
                });
                threads[threadCount].start(); //Starting each thread here
            	threadCount++;
	    }
            for (int j=0; j < threadCount; j++){ // TO-DO join all of the worker threads
                try {
                    threads[j].join();
                } catch (InterruptedException e) { e.printStackTrace(); }
            }
        }
        long endTime = System.currentTimeMillis();
        double executionTime = (endTime - startTime) / 1000.0;
        System.out.println("Indexing is finished. Total bytes indexed: " + bytesIndexed);
        System.out.println("Indexing took " + executionTime + " seconds.");

        return new IndexResult(executionTime, bytesIndexed);
        // TO-DO get the stop time and calculate the execution time
        // TO-DO return the execution time and the total number of bytes read

    }
    private HashMap<String, Long> getWordFrequencies(String filePath) { // TO-DO for each file extract all alphanumeric terms that are larger than 2 characters
        HashMap<String, Long> wordCounts = new HashMap<>();             //       and count their frequencies
        List<String> stopwords = Arrays.asList("and", "the", "is", "in", "of", "a", "to", "it");
        try {
            // Check if the file is a .DS_Store or other system files and skip it
            if (filePath.endsWith(".DS_Store")) {
                System.out.println("Skipping system file: " + filePath);
                return wordCounts;
            } // For below line, .lines method takes in the file path and returns the lines from the file as a stream.
            try (Stream<String> lines = Files.lines(Paths.get(filePath))) { // This is the main logic of getting word frequency
                lines.forEach(line -> //Performing an operation for each element of this stream of lines.
                        Arrays.stream(line.split("[^a-zA-Z0-9]+"))
                                .map(String::toLowerCase)
                                .filter(word -> word.length() > 2) //filtering only those words that have a length greater than 2.
                                .filter(word -> !stopwords.contains(word)) //filtering those word which are not stop words.
                                .forEach(word -> wordCounts.merge(word, 1L, Long::sum))
                );
            }
        } catch (MalformedInputException e) {
            System.err.println("Skipping file due to encoding issues: " + filePath);
        }

        catch (IOException e) {
            System.err.println("Error reading file: " + filePath);
        }
        return wordCounts;
    }
    public SearchResult searchFiles(ArrayList<String> terms) {
        SearchResult result = new SearchResult(0.0, new ArrayList<DocPathFreqPair>());
        // TO-DO get the start time
        double startTime = System.currentTimeMillis();
        if (terms.size() < 1) {
            throw new IllegalArgumentException("The search query must contain at least 1 term");
        }
        // TO-DO for each term get the pairs of documents and frequencies from the index store
        HashMap<Long, Long> documentFrequencyMap = new HashMap<>();
        for (String term : terms) {
            ArrayList<DocFreqPair> pairs = store.lookupIndex(term.toLowerCase());   // Ensure case insensitivity
            System.out.println("Printing term..." + term);
            for (DocFreqPair pair : pairs) {
                System.out.println("Printing document number..." + pair.documentNumber);
                System.out.println("Printing word frequency...." + pair.wordFrequency);
                documentFrequencyMap.merge(pair.documentNumber, pair.wordFrequency, Long::sum);
            }
        }
        // TO-DO combine the returned documents and frequencies from all of the specified terms
        // TO-DO for each document number get from the index store the document path
        ArrayList<DocPathFreqPair> results = new ArrayList<>(); // Populating our results here by adding objects containing docPath and word frequency into the array list.
        for (Map.Entry<Long, Long> entry : documentFrequencyMap.entrySet()) {
            String docPath = store.getDocument(entry.getKey());
            if (docPath != null) {
                results.add(new DocPathFreqPair(docPath, entry.getValue()));
            }
        }
        // TO-DO sort the document and frequency pairs and keep only the top 10
        results = results.stream() // Returning a stream or list of top 10 docs according to word frequency here.
                .sorted((a, b) -> Long.compare(b.wordFrequency, a.wordFrequency))
                .limit(10)
                .collect(Collectors.toCollection(ArrayList::new));
        // TO-DO get the stop time and calculate the execution time
        double endTime = System.currentTimeMillis();
        double executionTime2 = (endTime - startTime) / 1000.0;
        result.excutionTime = executionTime2;
        result.documentFrequencies = new ArrayList<>(results);
        return new SearchResult(executionTime2, new ArrayList<>(results)); // TO-DO return the execution time and the top 10 documents and frequencies
    }
}
