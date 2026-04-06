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
    
    //size of 1 singular disk block in bytes amt
    private static final int BLOCK_SIZE = 4096;
    
    //each record has a size of 8 bytes (4 for key and 4 for data)
    private static final int RECORD_SIZE = 8;
    
    //amt of records that can be max in any singular disk block
    private static final int RECORDS_PER_BLK = BLOCK_SIZE / RECORD_SIZE;
    
    // Allocate 50,000 bytes of working memory
    private byte[] workingMem = new byte[MEMBYTES];
    
    //num records currently in the memory 
    private int numRecords = 0;

    /**
     * Create a new ExternalSort object.
     * @param theFileName The name of the file to be sorted
     *
     * @throws IOException
     */
    public static void sort(String theFileName)
        throws IOException
    {
        ExternalSort sorter = new ExternalSort();
        RandomAccessFile theFile = new RandomAccessFile(theFileName, "rw");

        // Allocate 50,000 bytes of working memory
        //byte[] workingMem = new byte[MEMBYTES];

        // reads the binary file in
        sorter.readFile(theFile);
        
        // making an empty array for the records to 
        // be stored in with the initial size of the amt of records
        // accounted for from the sorter
        Record[] records = new Record[sorter.numRecords];
        ByteBuffer buffer = ByteBuffer.wrap(sorter.workingMem, 
            0, MEMBYTES - BLOCK_SIZE); 
                // only 1 block is used for the output buffer
        
        // going thru the sorter and fills the record array from 
        // the memory's stored records
        for (int i = 0; i < sorter.numRecords; i++) {
            byte[] aRecord = new byte[RECORD_SIZE];
            buffer.get(aRecord);
            records[i] = new Record(aRecord);
        }
        
        // organizing the records using the min-heap sort
        MinHeap heap = new MinHeap(records, 
            sorter.numRecords, sorter.numRecords);
        
        // once sorted they must go back into the memory sorted
        ByteBuffer output = ByteBuffer.wrap(sorter.workingMem, 
            0, RECORDS_PER_BLK);
        while (heap.heapSize() > 0) {
            Record minRecord = heap.removeMin();
            output.putInt(minRecord.getKey());
            output.putInt(minRecord.getValue());
        }
        
        // need to writeFile then close it
        sorter.writeFile(theFile);
        theFile.close();
        
        // setting up output buffer for next bytes of data
        output.flip();
        output.clear();
    }
    
    /**
     * Reads theFile file and puts it into the memory pool.
     * 
     * @param file is the file that is getting read
     * @throws IOException when anything fails to be read
     */
    public void readFile(RandomAccessFile file) throws IOException {
        file.seek(0);
        int readBytes = file.read(workingMem, 0, MEMBYTES);
        // cannot be negative number 
        // -> change readBytes to just threshold minimum of 0
        if (readBytes < 0) {
            readBytes = 0;
        }
        numRecords = readBytes / RECORD_SIZE;
    }
    
    /**
     * Writes the memory pool to the file by one block each.
     * 
     * @param file is the file to write into
     * @throws IOException when anything fails to be written
     */
    public void writeFile(RandomAccessFile file) throws IOException {
        int totalBytesFound = numRecords * RECORD_SIZE;
        int numWritten = 0;
        file.seek(0);
        while (numWritten < totalBytesFound) {
            // making it easier by taking it sections at a time to 
            // write it into the file
            int section = Math.min(BLOCK_SIZE, totalBytesFound - numWritten);
            file.write(workingMem, numWritten, section);
            // account for the amt of bytes now written into file
            numWritten += section; 
        }
    }
}
