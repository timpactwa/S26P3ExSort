import java.nio.*;
import java.io.*;

// The External Sort implementation
// -------------------------------------------------------------------------
/**
 *
 * @author {Your Name Here}
 * @version Spring 2026
 */
public class ExternalSort {

    /**
     * The working memory available to the program: 50,000 bytes
     */
    private static final int MEMBYTES = 50000;

    /**
     * Create a new ExternalSort object.
     * @param theFileName The name of the file to be sorted
     *
     * @throws IOException
     */
    public static void sort(String theFileName)
        throws IOException
    {
        RandomAccessFile theFile = new RandomAccessFile(theFileName, "rw");

        // Allocate 50,000 bytes of working memory
        byte[] workingMem = new byte[MEMBYTES];

    }
}
