package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.imageio.IIOException;
import javax.xml.namespace.QName;

public class FileServer {

    private FileSystemManager fsManager;
    private int port;

    // Handles client connections (threads)
    private final ThreadPoolExecutor pool;

    //Generate unique thread ID's for naming
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public FileServer(int port, String fileSystemName, int totalSize) {
        // Initialize the FileSystemManager
        this.fsManager = FileSystemManager.getInstance(fileSystemName, totalSize);
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

        //Ensure filesystem is closed on shutdown
        Runtime.getRuntime().addShutdownHook( new Thread(() -> {
            try {
                if (this.fsManager != null) {
                    this.fsManager.close();
                }
            } catch (Exception ignored) {}
        }));
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(12345)) {
            System.out.println("Server started. Listening on port 12345...");

            while (true) {
                //accepts client request and hand it off to client handler
                Socket clientSocket = serverSocket.accept();
                System.out.println("Handling client: " + clientSocket);
                this.pool.execute(new ClientTask(clientSocket));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        } finally {
            //ensure pool shutdown and filesystem closed
            try {this.pool.shutdown();
            } catch (Exception ignored) {}
            try {    
            if (this.fsManager != null) {
                this.fsManager.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing FileSystemManager: " + e.getMessage());
        }
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
            if (rejected) return ; // Task was rejected, do not proceed}
            System.out.println(" Handling client in: " + Thread.currentThread().getName() + "-" + clientSocket);

            try (
                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())); 
                     PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
                ) {  String line;
                     while ((line = reader.readLine()) != null) {
                    System.out.println("Received from client: " + line);
                    
                    try {
                    // Trim and validate command
                    if (line == null ){
                        writer.println("ERROR: empty command");
                        continue;
                    }
                    line=line.trim();
                    if(line.isEmpty()){
                        writer.println("ERROR: Empty command");
                        continue;
                    }

                    if(FileServer.this.fsManager == null){
                        writer.println("ERROR: Filesystem not initialized");
                        continue;
                    }

                    String[] parts = line.split(" ");
                    String command = parts[0].toUpperCase();

                    switch (command) {
                        case "CREATE":
                            if (parts.length < 2) {
                                writer.println("ERROR: Filename required.");
                                break;
                            }
                           try {
                                fsManager.createFile(parts[1]);
                                writer.println("SUCCESS: File '" + parts[1] + "' created.");
                           } catch (Exception e) {
                                writer.println("ERROR: " + e.getMessage());
                           }
                            break;

                        case "READ":
                            if (parts.length < 2) {
                                writer.println("ERROR: Filename required.");
                                break;
                            }
                            try {
                                byte[] data = fsManager.readFile(parts[1]);
                                if (data == null) {
                                    writer.println("ERROR: File not found.");
                                } else {
                                    //Convert bytes to string using UTF-8
                                    String content = new String(data, StandardCharsets.UTF_8);
                                    writer.println("SUCCESS: " + content);
                                }
                            } catch (Exception e) {
                                writer.println("ERROR: " + e.getMessage());
                            }
                            break;

                        case "WRITE":
                            if (parts.length < 3) {
                                writer.println("ERROR: Filename and data required.");
                                break;
                            }
                            try {
                                String fileName = parts[1];
                                String dataStr = line.substring(line.indexOf(fileName) + fileName.length()).trim();
                                byte[] data = dataStr.getBytes(); // Convert string to bytes
                                fsManager.writeFile(fileName, data);
                                writer.println("SUCCESS: File '" + fileName + "' written with: " + new String(data, StandardCharsets.UTF_8));
                            } catch (Exception e) {
                                writer.println("ERROR: " + e.getMessage());
                            }
                            break;

                        case "DELETE":
                            if (parts.length < 2) {
                                writer.println("ERROR: Filename required.");
                                break;
                            }
                            try {
                                fsManager.deleteFile(parts[1]);
                                writer.println("SUCCESS: File '" + parts[1] + "' deleted.");
                            } catch (Exception e) {
                                writer.println("ERROR: " + e.getMessage());
                            }
                            break;
                        case "LIST":
                            try {
                                String[] files = fsManager.listFiles();
                                writer.println("SUCCESS: Files found: " + String.join(", ", files));
                            } catch (Exception e) {
                                writer.println("ERROR: " + e.getMessage());
                            }
                            break;
                        case "QUIT":
                            writer.println("SUCCESS: Disconnecting.");
                            return;
                        default:
                            writer.println("ERROR: Unknown command.");
                            break;
                  }
                    } catch (Exception perLException) {
                        writer.println("ERROR: Malformed input" + perLException.getMessage());
                    }
                }
                
            } catch (IOException e) {
                System.err.println("I/O error handling client: " + clientSocket + " " + e.getMessage());
            }  catch (RuntimeException e) {
                System.err.println("Runtime error handling client: "+ clientSocket + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException ignored) {
                    // Ignore
                }
            }
        }
    }

}
