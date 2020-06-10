package com.webutils.storageserver.http;

import com.webutils.webserver.http.HttpInfo;
import com.webutils.webserver.requestcontext.RequestContext;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class ChunkFileHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ChunkFileHandler.class);

    private final RequestContext requestContext;

    public ChunkFileHandler(final RequestContext requestContext) {
        this.requestContext = requestContext;
    }

    public FileChannel getWriteFileChannel() {
        FileChannel writeFileChannel;

        String filePathNameStr = buildChunkFileName();
        if (filePathNameStr == null) {
            return null;
        }

        /*
         ** Open up the File for writing
         */
        File outFile = new File(filePathNameStr);
        try {
            writeFileChannel = new FileOutputStream(outFile, false).getChannel();
        } catch (FileNotFoundException ex) {
            LOG.info("WriteToFile[" + requestContext.getRequestId() + "] file not found: " + ex.getMessage());
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to open file - " + filePathNameStr + "\"" +
                    "\r\n}";
            requestContext.getHttpInfo().emitMetric(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            requestContext.getHttpInfo().setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412);
            writeFileChannel = null;
        }

        return writeFileChannel;
    }

    public FileChannel getReadFileChannel() {
        FileChannel readFileChannel;

        String filePathNameStr = buildChunkFileName();

        File inFile = new File(filePathNameStr);
        try {
            readFileChannel = new FileInputStream(inFile).getChannel();

        } catch (FileNotFoundException ex) {
            LOG.info("ReadFromFile[" + requestContext.getRequestId() + "] file not found: " + ex.getMessage());

            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Unable to open file - " + filePathNameStr + "\"" +
                    "\r\n}";
            requestContext.getHttpInfo().emitMetric(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            requestContext.getHttpInfo().setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412);

            readFileChannel = null;
        }

        return readFileChannel;
    }

    public void close(final FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ex) {
                LOG.info("close[" + requestContext.getRequestId() + "] close exception: " + ex.getMessage());
            }
        }
    }

    public void deleteFile() {
        String fileName = buildChunkFileName();
        if (fileName != null) {
            LOG.warn("Deleting file: " + fileName + " error(may not be one): " + requestContext.getHttpParseStatus());

            Path filePath = Paths.get(fileName);
            try {
                Files.deleteIfExists(filePath);
            } catch (IOException ex) {
                LOG.warn("Failure deleting file: " + fileName + " exception: " + ex.getMessage());
            }
        }
    }

    /*
     ** This builds the filePath to where the chunk of data will be saved.
     **
     ** It is comprised of the chunk location, chunk number, chunk lba and located at
     **   ./logs/StorageServer"IoInterfaceIdentifier"/chunk_"chunk location"_"chunk lba".dat
     */
    public String buildChunkFileName() {
        int chunkLba = requestContext.getHttpInfo().getObjectChunkLba();
        String chunkLocation = requestContext.getHttpInfo().getObjectChunkLocation();

        if ((chunkLba == -1) || (chunkLocation == null)) {
            LOG.error("WriteToFile chunkLba: " + chunkLba + " chunkLocation: " + chunkLocation);
            String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                    "\r\n  \"message\": \"Missing chunk attributes\"" +
                    "\r\n  \"" + HttpInfo.CHUNK_LBA + "\": \"" + chunkLba + "\"" +
                    "\r\n  \"" + HttpInfo.CHUNK_LOCATION + "\": \"" + Objects.requireNonNullElse(chunkLocation, "null") + "\"" +
                    "\r\n}";
            requestContext.getHttpInfo().emitMetric(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
            requestContext.getHttpInfo().setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412);
            return null;
        }

        /*
         ** Check if the directory exists
         */
        String directory = "./logs/StorageServer" + requestContext.getIoInterfaceIdentifier();
        File tmpDir = new File(directory);
        if (!tmpDir.exists()) {
            try {
                Files.createDirectories(Paths.get(directory));
            } catch (IOException ex) {
                LOG.error("Unable to create directory - " + directory);
                String failureMessage = "{\r\n  \"code\":" + HttpStatus.PRECONDITION_FAILED_412 +
                        "\r\n  \"message\": \"Unable to create directory - " + directory + "\"" +
                        "\r\n}";
                requestContext.getHttpInfo().emitMetric(HttpStatus.PRECONDITION_FAILED_412, failureMessage);
                requestContext.getHttpInfo().setParseFailureCode(HttpStatus.PRECONDITION_FAILED_412);

                return null;
            }
        }

        return directory + "/chunk_" + chunkLocation + "_" + chunkLba + ".dat";
    }

}
