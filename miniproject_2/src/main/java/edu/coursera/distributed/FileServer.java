package edu.coursera.distributed;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.nio.file.Files;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {
    /**
     * Main entrypoint for the basic file server.
     *
     * @param socket Provided socket to accept connections on.
     * @param fs A proxy filesystem to serve files from. See the PCDPFilesystem
     *           class for more detailed documentation of its usage.
     * @throws IOException If an I/O error is detected on the server. This
     *                     should be a fatal error, your file server
     *                     implementation is not expected to ever throw
     *                     IOExceptions during normal operation.
     */
    public void run(final ServerSocket socket, final PCDPFilesystem fs)
            throws IOException {
        /*
         * Enter a spin loop for handling client requests to the provided
         * ServerSocket object.
         */
        while (true) {
            // Single Threaded Version
            Socket s = socket.accept();

            InputStream inputstream = s.getInputStream();
            InputStreamReader reader = new InputStreamReader(inputstream);
            BufferedReader bufferedReader = new BufferedReader(reader);

            String line = bufferedReader.readLine();
            assert line!=null;
            assert line.startsWith("GET");
            final PCDPPath path = new PCDPPath(line.split(" ")[1]);
            final String file = fs.readFile(path);

            OutputStream out = s.getOutputStream();
            PrintWriter printWriter = new PrintWriter(out);

            if (file != null) {
                printWriter.write("HTTP/1.0 200 OK\r\n");
                printWriter.write("Server: FileServer\r\n");
                printWriter.write("\r\n");
                printWriter.write(file + "\r\n");
                printWriter.flush();
            } else {
                printWriter.write("HTTP/1.0 404 Not Found\r\n");
                printWriter.write("Server: FileServer\r\n");
                printWriter.write("\r\n");
                printWriter.flush();
                //  Buffered Writer doest push data untill buffer full or forced via flush()
            }
            out.close();    //Close the Stream, Flushes all data in buffer before closing
        }
    }
}
