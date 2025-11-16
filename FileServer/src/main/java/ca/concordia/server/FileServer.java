package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FileServer {

    private FileSystemManager fsManager;
    private int port;

    // Handles client connections (threads)
    private final ThreadPoolExecutor pool;

    //Generate unique thread ID's for naming
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public FileServer(int port, String fileSystemName, int totalSize) {
        // Initialize the FileSystemManager
        FileSystemManager fsManager = FileSystemManager.getInstance(fileSystemName, totalSize);
        this.port = port;

        // ThreadPool configuration
        int poolSize = 50; // min threads
        int maxPoolSize = 500; // max threads
        long keepAliveSeconds = 60; // Time for idle threads to live
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(1000); // Bounded Queue for incoming tasks

        // Creates, names and return created threads
        ThreadFactory threadFactory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "FileServer-Worker-" + threadNumber.getAndIncrement());
            }
        };

        // Notifies client when server is saturated
        RejectedExecutionHandler rejectedHandler = (r, executor) -> {
            if (r instanceof ClientTask) {
                ((ClientTask) r).reject(); // server busy, close socket
            } else {
                System.err.println("Task " + r.toString() + " rejected ");
            }
        };

        // Creates ThreadPoolExecutor with defined constraints
        this.pool = new ThreadPoolExecutor(
                poolSize,
                maxPoolSize,
                keepAliveSeconds, TimeUnit.SECONDS,
                workQueue,
                threadFactory,
                rejectedHandler
        );
        this.pool.allowCoreThreadTimeOut(true); // Allow core threads to time out
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(12345)) {
            System.out.println("Server started. Listening on port 12345...");

            while (true) {
                //accepts client request and
                Socket clientSocket = serverSocket.accept();
                System.out.println("Handling client: " + clientSocket);
                this.pool.execute(new ClientTask(clientSocket));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        } finally {
            this.pool.shutdown();
        }
    }

    // Handles each working threads
    private class ClientTask implements Runnable {

        private final Socket clientSocket;
        private boolean rejected = false;

        public ClientTask(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        // Called by RejectedExecutionHandler when task is rejected
        public void reject() {
            this.rejected = true;
            try {
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                out.println("Server is busy. Please try again later.");
                clientSocket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            if (rejected) {
                return; // Task was rejected, do not proceed

                        }System.out.println("Handling client in: " + Thread.currentThread().getName() + " - " + clientSocket);

            try (
                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())); PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("Received from client: " + line);
                    String[] parts = line.split(" ");
                    String command = parts[0].toUpperCase();

                    switch (command) {
                        case "CREATE":
                            fsManager.createFile(parts[1]);
                            writer.println("SUCCESS: File '" + parts[1] + "' created.");
                            writer.flush();
                            break;
                        //TODO: Implement other commands READ, WRITE, DELETE, LIST
                        case "QUIT":
                            writer.println("SUCCESS: Disconnecting.");
                            return;
                        default:
                            writer.println("ERROR: Unknown command.");
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
    }

}
