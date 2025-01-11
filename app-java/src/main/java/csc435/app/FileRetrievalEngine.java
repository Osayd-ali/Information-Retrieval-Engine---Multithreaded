package csc435.app;

public class FileRetrievalEngine
{
    public static void main(String[] args)
    {
        int numWorkerThreads = 1;

        // TO-DO initialize the number of worker threads from args[0]
        if (args.length > 0) {
            try {
                numWorkerThreads = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid number of worker threads. Using default value of 1.");
            }
        } else {
            System.out.println("Number of worker threads not provided. Using default value of 1.");
        }

        IndexStore store = new IndexStore();
        ProcessingEngine engine = new ProcessingEngine(store, numWorkerThreads);
        AppInterface appInterface = new AppInterface(engine);

        appInterface.readCommands();
    }
}
