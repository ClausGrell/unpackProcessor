package grell.processors.demo;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipOperations {
    public static FlowFile getNextFile(FlowFile flowFile, ZipEntry zipEntry, ZipInputStream zstdInputStream, ProcessSession session) {
        FlowFile newFlowFile = session.create(flowFile);
        OutputStream outputStream = session.write(newFlowFile);

        try {
            FileOutputStream fos = null;
            try {
                //                    outputStream = new outputStream(entry.getName());
                final byte[] buf = new byte[1024];
                int read = 0;
                int length;
                while ((length = zstdInputStream.read(buf, 0, buf.length)) >= 0) {
                    outputStream.write(buf, 0, length);
                }
            } catch (IOException ioex) {
                fos.close();
            }

            outputStream.close();
            var lastModifiedTime = String.valueOf(zipEntry.getLastModifiedTime());
            var creationTime = String.valueOf(zipEntry.getCreationTime());
            var lastAccessTime = String.valueOf(zipEntry.getLastAccessTime());
            String directoryPath = zipEntry.getName().substring(0, zipEntry.getName().lastIndexOf('/'));
            var filename = zipEntry.getName().substring(zipEntry.getName().lastIndexOf('/') + 1);

            if (creationTime.equals("null")) creationTime = lastModifiedTime;
            if (lastAccessTime.equals("null")) lastAccessTime = lastModifiedTime;


            session.putAttribute(newFlowFile, "file.creationTime", String.valueOf(creationTime));
            session.putAttribute(newFlowFile, "file.lastModifiedTime", String.valueOf(lastModifiedTime));
            session.putAttribute(newFlowFile, "file.lastAccessTime", String.valueOf(lastAccessTime));
            session.putAttribute(newFlowFile, "filename", filename);
            session.putAttribute(newFlowFile, "path", directoryPath);
            return newFlowFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
