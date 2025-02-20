package grell.processors.demo;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.stream.io.StreamUtils;
import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZstdOperations {

    public void compress() {
        byte[] buff = new byte[1024];

        try {
            ZstdOutputStream zstdOutputStream = new ZstdOutputStream(new FileOutputStream("Compressed.zst"));
            FileInputStream fileInputStream = new FileInputStream("UncompressedText.txt");
            int length;
            while ((length = fileInputStream.read(buff)) > 0) {
                zstdOutputStream.write(buff, 0, length);
            }
            fileInputStream.close();
            zstdOutputStream.close();
        } catch (IOException exception) {
            System.out.println("Encountered an exception while compressing the file :" + exception);
        }
    }

    public FlowFile decompress(FlowFile inFlowFile, ProcessSession session) throws IOException {
        FlowFile newFlowFile = session.create(inFlowFile);
        InputStream inputStream = session.read(inFlowFile);
        ZstdInputStream zstdInputStream = new ZstdInputStream(inputStream);

        OutputStream outputStream = session.write(newFlowFile);
        StreamUtils.copy(zstdInputStream, outputStream);


        zstdInputStream.close();
        outputStream.close();

        String fileName = newFlowFile.getAttribute("filename");
        String decompressedFilename = fileName.substring(0, fileName.lastIndexOf('.'));
        newFlowFile = session.putAttribute(newFlowFile, "zstd.decompressed", "true");
        newFlowFile = session.putAttribute(newFlowFile, "original.filename", fileName);
        newFlowFile = session.putAttribute(newFlowFile, "decompressed.filename", decompressedFilename);
        newFlowFile = session.putAttribute(newFlowFile, "compression.method", "zstd");
        return newFlowFile;
    }

    public void unzip() {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream("src/test.zip");
            ZipInputStream zis = new ZipInputStream(fis);
            ZipEntry je = zis.getNextEntry();

            var modifyTime = je.getLastModifiedTime();
            var creationTime = je.getCreationTime();
            var aa = je.getLastAccessTime();
            extractFile(je, zis);

            je = zis.getNextEntry();

            extractFile(je, zis);

            System.out.println("aa = " + aa);
            System.out.println("modifyTime = " + modifyTime);
            System.out.println("creationTime = " + creationTime);

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void extractFile(ZipEntry entry, ZipInputStream is)
            throws IOException {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(entry.getName());
            final byte[] buf = new byte[1024];
            int read = 0;
            int length;
            while ((length = is.read(buf, 0, buf.length)) >= 0) {
                fos.write(buf, 0, length);
            }
        } catch (IOException ioex) {
            fos.close();
        }
    }

}

