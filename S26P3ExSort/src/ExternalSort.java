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
            // - one block to account for output buffer's size
        
        // going thru the sorter and fills the record array from 
        // the memory's stored records
        for (int i = 0; i < sorter.numRecords; i++) {
            byte[] aRecord = new byte[RECORD_SIZE];
            buffer.get(aRecord);
            records[i] = new Record(aRecord);
        }
        
        while (theFile.getFilePointer() < theFile.length()) {
            // organizing the records using the min-heap sort
            MinHeap heap = new MinHeap(records, 
                sorter.numRecords, MEMBYTES - BLOCK_SIZE);
            
            // once sorted they must go back into the memory sorted
            // buffers every 4096 bytes (1 block size) aka 512 records
            int heapBytes = 0;
            while (heapBytes + BLOCK_SIZE <= heapCapacity 
                && theFile.getFilePointer() < theFile.length()) {
                
                // reading in a block of records from the memory
                theFile.read(sorter.workingMem, heapOffSet + heapBytes, BLOCK_SIZE);
                heapBytes += BLOCK_SIZE;
                
                // making output buffer
                ByteBuffer output = ByteBuffer.wrap(sorter.workingMem, 
                    0, RECORDS_PER_BLK);
                // putting the mins from the heap into the buffer until 
                // it is full or the heap is empty
                while (heap.heapSize() > 0) {
                    if (output.remaining() == 0) {
                        output.clear(); // clears buffer and sets position back to 0
                    }
                    // putting min from heap into the output buffer
                    Record minRecord = heap.removeMin();
                    output.putInt(minRecord.getKey());
                    output.putInt(minRecord.getValue());
                }
                // setting up output buffer for next bytes of data
                output.flip();
                output.clear();
            }
        }
        
        theFile.close();
    }
    
    /**
     * Reads theFile file and puts it into the memory pool.
     * 
     * @param file is the file that is getting read
     * @throws IOException when anything fails to be read
     */
    public void readFile(RandomAccessFile file) throws IOException {
        // reset file pointer
        file.seek(0);
        
        int totalRead = 0;
        
        // read up to either the length of the file or the max memory pool
        int fileSize = (int) file.length();
        int toRead = Math.min(fileSize, MEMBYTES);
        
        // make sure to read entire file past first 2048 bytes
        while (totalRead < toRead) {
            int readBytes = file.read(workingMem, totalRead, 
                toRead - totalRead);
            
            // nothing found case
            if (readBytes < 0) break;
            
            totalRead += readBytes;
        }
        
        numRecords = totalRead / RECORD_SIZE;
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
