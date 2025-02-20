package grell.processors.demo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class DecompressZip {
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
