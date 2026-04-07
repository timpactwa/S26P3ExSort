import java.nio.*;
import java.io.*;

// The External Sort implementation
// -------------------------------------------------------------------------
/**
 * @author Brianna McDonald, Tim Pactwa
 * @version Spring 2026
 */
public class ExternalSort {

    /** max size of the working memory pool in bytes */
    private static final int MEMBYTES = 50000;
    /** size of a single record in bytes: 4 bytes for key and 4 for value */
    private static final int RECORD_SIZE = 8;
    /** block size in bytes for one disk */
    private static final int BLOCK_SIZE = 4096;
    /** the working memory pool */
    private byte[] workingMem = new byte[MEMBYTES];

    /**
     * Create a new ExternalSort object.
     * 
     * @param theFileName
     *            The name of the file to be sorted
     * @throws IOException
     */
    public static void sort(String theFileName) throws IOException {
        // ---------------------------------------------------------------
        // SETUP
        ExternalSort sorter = new ExternalSort();
        File f = new File(theFileName);
        long originalFileSize = f.length();

        // output buffer is at the last block of the memory pool
        int outputBufStart = MEMBYTES - BLOCK_SIZE;
        // heap is in memory pool before the output buffer
        int heapAreaSize = outputBufStart;
        // num records that the heap can hold
        int heapRecordCap = heapAreaSize / RECORD_SIZE;
        // amt of bytes the heap can hold
        int heapBytesCap = heapRecordCap * RECORD_SIZE;

        // making the files for reading
        RandomAccessFile theFile =
            new RandomAccessFile(theFileName, "rw");
        RandomAccessFile tempFile =
            new RandomAccessFile("tempSortFile.bin", "rw");
        tempFile.setLength(0);

        // ---------------------------------------------------------------
        // HEAP SORTING: putting the input file data into the heap and then
        // sorting it in the heap to make it a minheap
        
        int numRuns = 0;
        long bytesRead = 0;

        while (bytesRead < originalFileSize) {
            // reading the file size and stopping when the heap bytes cap
            // has been reached
            int bytesToRead = (int)Math.min(
                heapBytesCap, originalFileSize - bytesRead);
            theFile.seek(bytesRead);
            theFile.readFully(sorter.workingMem, 0, bytesToRead);

            int numRecs = bytesToRead / RECORD_SIZE;

            // making the heap
                // heap gets sorted upon initialization in the 
                // constructor of the minheap class
            MinHeap heap = new MinHeap(
                sorter.workingMem, 0, numRecs, numRecs);

            // gets the sorted records from the heap and puts 
            // it into the output buffer, then it will flush the 
            // records from the buffer to the disk when it is full
            sorter.extractAndWrite(heap, tempFile, bytesRead,
                numRecs, outputBufStart);

            // updating counter of # of bytes read already and 
            // the counter for the number of runs completed
            bytesRead += bytesToRead;
            numRuns++;
        }
        // ---------------------------------------------------------------
        // MULTI WAY MERGING
        
        // multi-merge sorts when there is more than 1 run that occurred
        if (numRuns > 1) {
            sorter.mergeRuns(tempFile, theFile, numRuns,
                originalFileSize, heapBytesCap);
        }
        else { 
            // only 1 run executed -> moving all the records 
            // from the 1 buffer to the output file
            theFile.seek(0);
            tempFile.seek(0);
            long remaining = originalFileSize; // holder for # bytes to move over
            // loops until the remaining variable is at 0, 
            // meaning all bytes from the file size have been moved over in 
            // increments of block size (4096 bytes at a time)
            while (remaining > 0) {
                int toRead = (int)Math.min(BLOCK_SIZE, remaining);
                tempFile.readFully(sorter.workingMem, 0, toRead);
                theFile.write(sorter.workingMem, 0, toRead);
                // updating holder
                remaining -= toRead;
            }
        }

        theFile.close();
        tempFile.close();
        new File("tempSortFile.bin").delete();
    }

    /**
     * Taking all the sorted records from the minheap and
     * writes them to outFile using a block-sized (4096 bytes) output buffer
     *
     * @param heap           the minHeap to extract from
     * @param outFile        output file
     * @param fileOffset     byte position in outFile to start writing
     * @param numRecs        number of records to extract from the heap
     * @param outputBufStart offset in workingMem for the output buffer
     * 
     * @throws IOException if file I/O fails
     */
    private void extractAndWrite(
        MinHeap heap, RandomAccessFile outFile, long fileOffset,
        int numRecs, int outputBufStart) throws IOException {

        outFile.seek(fileOffset);
        int bufferIndex = 0;

        // loops through the records in the heap and keeps taking 
        // out the min record, writing it into the output file
        for (int i = 0; i < numRecs; i++) {
            Record min = heap.removeMin();
            byte[] data = min.toBytes();
            System.arraycopy(data, 0, workingMem, outputBufStart + bufferIndex,
                RECORD_SIZE);
            bufferIndex += RECORD_SIZE;

            // flushing buffer because it's full (has 4096 bytes)
            if (bufferIndex >= BLOCK_SIZE) {
                outFile.write(workingMem, outputBufStart, bufferIndex);
                bufferIndex = 0;
            }
        }
        // flush leftover data
        if (bufferIndex > 0) {
            outFile.write(workingMem, outputBufStart, bufferIndex);
        }
    }

    /**
     * Merging the runs together from the source into the outFile. Divides
     * the workingMem pool and compares the different buffers and puts it
     * into the workingMem.
     *
     * @param in        file containing sorted runs
     * @param outFile   file to write the merged output onto
     * @param numRuns   number of runs executed
     * @param totalBytes total bytes across all runs
     * @param maxRunSize  max size of each run in bytes
     * @throws IOException if file I/O fails
     */
    private void mergeRuns(
        RandomAccessFile in, RandomAccessFile outFile,
        int numRuns, long totalBytes,
        int maxRunSize) throws IOException {

        // dividing working memory pool
        int perRunBufSize = MEMBYTES / (numRuns + 1);
        // taking in the boundaries of the buffer
        perRunBufSize = (perRunBufSize / RECORD_SIZE) * RECORD_SIZE;
        if (perRunBufSize < RECORD_SIZE) {
            perRunBufSize = RECORD_SIZE;
        }

        // last piece of memory is saved for the output buffer
        int outputBufStart = numRuns * perRunBufSize;
        int outputBufSize = MEMBYTES - outputBufStart;
        int outputBufIndex = 0;

        // file pointers and buffer indexes for each run
        long[] runFilePtr = new long[numRuns];
        long[] runEnd = new long[numRuns];
        int[] bufBytes = new int[numRuns];
        int[] bufNext = new int[numRuns];
        boolean[] runDone = new boolean[numRuns];

        // looping thru all the runs' buffers and setting values
        for (int i = 0; i < numRuns; i++) {
            runFilePtr[i] = (long)i * maxRunSize;
            runEnd[i] = Math.min(runFilePtr[i] + maxRunSize, totalBytes);
            bufBytes[i] = 0;
            bufNext[i] = 0;
            runDone[i] = false;
        }

        outFile.seek(0);
        int completedRuns = 0;

        // going through the runs and getting the mins from each buffer 
        // until every buffer is used up
        while (completedRuns < numRuns) {
            int minIndex = -1;
            int minValsKey = Integer.MAX_VALUE;

            for (int i = 0; i < numRuns; i++) {
                if (runDone[i]) {
                    continue;
                }

                // refilling the current run's buffer from disk if exhausted
                if (bufNext[i] >= bufBytes[i]) {
                    if (runFilePtr[i] >= runEnd[i]) {
                        // hits this code when the run is fully used
                        runDone[i] = true;
                        completedRuns++;
                        continue;
                    }
                    int toRead = (int)Math.min(
                        perRunBufSize, runEnd[i] - runFilePtr[i]);
                    in.seek(runFilePtr[i]);
                    in.readFully(
                        workingMem, i * perRunBufSize, toRead);
                    bufBytes[i] = toRead;
                    bufNext[i] = 0;
                    runFilePtr[i] += toRead;
                }

                // comparing the current run's current record key
                int off = (i * perRunBufSize) + bufNext[i];
                int key = ByteBuffer.wrap(
                    workingMem, off, 4).getInt();
                if (key < minValsKey) {
                    minValsKey = key;
                    minIndex = i;
                }
            }

            if (minIndex == -1) {
                break; // all runs have been looked at
            }

            // moving the min record into the output buffer
            int offset = (minIndex * perRunBufSize) + bufNext[minIndex];
            System.arraycopy(
                workingMem, offset,
                workingMem, outputBufStart + outputBufIndex,
                RECORD_SIZE);
            outputBufIndex += RECORD_SIZE;
            bufNext[minIndex] += RECORD_SIZE;

            // flushes the buffer if it's full
            if (outputBufIndex >= outputBufSize) {
                outFile.write(workingMem, outputBufStart, outputBufIndex);
                outputBufIndex = 0;
            }
        }

        // flushing out any leftover data
        if (outputBufIndex > 0) {
            outFile.write(workingMem, outputBufStart, outputBufIndex);
        }
    }
}
