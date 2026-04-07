import java.nio.*;
import java.io.*;

/**
 * The External Sort implementation
 * -------------------------------------------------------------------------
 * 
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
     * @param theFileName
     *            The name of the file to be sorted
     * @throws IOException
     */
    public static void sort(String theFileName) throws IOException {
        ExternalSort sorter = new ExternalSort();
        File f = new File(theFileName);
        long fileSize = f.length();

        // output buffer is at the last block of the memory pool
        int outputBufStart = MEMBYTES - BLOCK_SIZE;
        // heap is in memory pool before the output buffer
        int heapAreaSize = outputBufStart;
        // num records that the heap can hold
        int heapRecordCap = heapAreaSize / RECORD_SIZE;
        // amt of bytes the heap can hold
        int heapBytesCap = heapRecordCap * RECORD_SIZE;

        // making the files for reading
        RandomAccessFile theFile = new RandomAccessFile(theFileName, "rw");
        RandomAccessFile tempFile = new RandomAccessFile("tempSortFile.bin",
            "rw");
        tempFile.setLength(0);

        // ---------------------------------------------------------------
        // HEAP SORTING: putting the input file data into the heap and then
        // sorting it in the heap to make it a minheap
        
        int numRuns = 0;
        long bytesRead = 0;

        while (bytesRead < fileSize) {
            // reading the file size and stopping when the heap bytes cap
            // has been reached
            int bytesToRead = (int)Math.min(heapBytesCap, fileSize - bytesRead);
            theFile.seek(bytesRead);
            theFile.readFully(sorter.workingMem, 0, bytesToRead);

            int numRecs = bytesToRead / RECORD_SIZE;
            
            // making the heap
                // heap gets sorted upon initialization in the 
                // constructor of the minheap class
            MinHeap heap = new MinHeap(sorter.workingMem, 0, numRecs, numRecs);

            // gets the sorted records from the heap and puts 
            // it into the output buffer, then it will flush the 
            // records from the buffer to the disk when it is full
            sorter.extractAndWrite(heap, tempFile, bytesRead, numRecs,
                outputBufStart);

            // updating counter of # of bytes read already and 
            // the counter for the number of runs completed
            bytesRead += bytesToRead;
            numRuns++;
        }

        // ---------------------------------------------------------------
        // MULTI WAY MERGING
        
        // multi-merge sorts when there is more than 1 run that occurred
        if (numRuns > 1) {
            sorter.mergeRuns(tempFile, theFile, numRuns, fileSize,
                heapBytesCap);
        }
        else {
            // only 1 run executed -> moving all the records 
            // from the 1 buffer to the output file
            theFile.seek(0);
            tempFile.seek(0);
            // holder for # bytes to move over
            long remaining = fileSize;
            
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
     * @param heap
     *            the minHeap to extract from
     * @param outFile
     *            output file
     * @param fileOffset
     *            byte position in outFile to start writing
     * @param numRecs
     *            number of records to extract from the heap
     * @param outputBufStart
     *            offset in workingMem for the output buffer
     * @throws IOException
     *             if file I/O fails
     */
    private void extractAndWrite(MinHeap heap, RandomAccessFile outFile,
        long fileOffset, int numRecs, int outputBufStart) throws IOException {

        outFile.seek(fileOffset);
        int bufferIndex = 0;

        // loops through the records in the heap and keeps taking 
        // out the min record, writing it into the output file
        for (int i = 0; i < numRecs; i++) {
            int srcOffset = heap.removeMinOffset();
            System.arraycopy(workingMem, srcOffset, workingMem, outputBufStart
                + bufferIndex, RECORD_SIZE);
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
     * @param in
     *            file containing sorted runs
     * @param outFile
     *            file to write the merged output onto
     * @param numRuns
     *            number of runs to merge
     * @param totalBytes
     *            total bytes across all runs
     * @param maxRunSize
     *            max size of each run in bytes
     * @throws IOException
     *             if file I/O fails
     */
    private void mergeRuns(RandomAccessFile in, RandomAccessFile outFile, 
        int numRuns, long totalBytes, int maxRunSize) throws IOException {

        // split memory to all run buffers plus one output buffer
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

        // looping thru all the runs' buffers and setting values
        for (int i = 0; i < numRuns; i++) {
            runFilePtr[i] = (long)i * maxRunSize;
            runEnd[i] = Math.min(runFilePtr[i] + maxRunSize, totalBytes);
            bufBytes[i] = 0;
            bufNext[i] = 0;
        }

        // going through the runs and getting the mins/first piece of data
        // from each buffer until every buffer is used up
        int activeRuns = 0;
        for (int i = 0; i < numRuns; i++) {
            // refilling the current run's buffer from disk if exhausted
            if (runFilePtr[i] < runEnd[i]) {
                int toRead = (int)Math.min(perRunBufSize, runEnd[i]
                    - runFilePtr[i]);
                in.seek(runFilePtr[i]);
                in.readFully(workingMem, i * perRunBufSize, toRead);
                bufBytes[i] = toRead;
                bufNext[i] = 0;
                runFilePtr[i] += toRead;
                activeRuns++;
            }
        }

        // filling the merge's heap with the first min from each run's buffer
        int heapSize = activeRuns;
        int[] mergeKeys = new int[heapSize];
        int[] mergeRun = new int[heapSize];
        int index = 0;
        for (int i = 0; i < numRuns; i++) {
            if (bufBytes[i] > 0) {
                int off = (i * perRunBufSize) + bufNext[i];
                mergeKeys[index] = getKeyAt(off);
                mergeRun[index] = i;
                index++;
            }
        }

        // making the merge's heap with the merge data
        buildMergeHeap(mergeKeys, mergeRun, heapSize);

        outFile.seek(0);

        // looping through the heap and moving the 
        // mins every iteration into the buffer
        while (heapSize > 0) {
            int minRunIndex = mergeRun[0];

            // copying the min record into the output buffer
            int offset = (minRunIndex * perRunBufSize) + bufNext[minRunIndex];
            System.arraycopy(workingMem, offset, workingMem, outputBufStart
                + outputBufIndex, RECORD_SIZE);
            outputBufIndex += RECORD_SIZE;
            bufNext[minRunIndex] += RECORD_SIZE;

            // flushing buffer when full
            if (outputBufIndex >= outputBufSize) {
                outFile.write(workingMem, outputBufStart, outputBufIndex);
                outputBufIndex = 0;
            }

            // boolean checker to see whether there are more 
            // records in the run's buffer
            boolean hasNext = advanceRun(in, minRunIndex, perRunBufSize,
                runFilePtr, runEnd, bufBytes, bufNext);
            // when checker is true, gets the the next record in the 
            // buffer and sifts down the merge
            if (hasNext) {
                // put the next record from this run at the top of the heap
                int offset2 = (minRunIndex * perRunBufSize) 
                    + bufNext[minRunIndex];
                mergeKeys[0] = getKeyAt(offset2);
                siftDownMerge(mergeKeys, mergeRun, heapSize, 0);
            }
            else {
                // current run is completed, moves the last heap entry up to 
                // fill the empty space, decrements the heap
                heapSize--;
                if (heapSize > 0) {
                    mergeKeys[0] = mergeKeys[heapSize];
                    mergeRun[0] = mergeRun[heapSize];
                    siftDownMerge(mergeKeys, mergeRun, heapSize, 0);
                }
            }
        }

        // flushing out leftover data from output buffer
        if (outputBufIndex > 0) {
            outFile.write(workingMem, outputBufStart, outputBufIndex);
        }
    }


    /**
     * Helper method that moves to the next record in the parameterized 
     * run's buffer. When the run's buffer is empty, it will read the 
     * next chunk from disk. Will return false when the run has no more 
     * records left in the buffer.
     *
     * @param in
     *            the input file to read from
     * @param runIndex
     *            which run to advance
     * @param perRunBufSize
     *            size of each run's in-memory buffer
     * @param runFilePtr
     *            current read position in the file for each run
     * @param runEndPos
     *            end position in the file for each run
     * @param bufBytes
     *            how many bytes are loaded in each run's buffer
     * @param bufNext
     *            current read offset within each run's buffer
     *            
     *            
     * @return true if a next record is available, false if the run is done
     * @throws IOException
     *             if file I/O fails
     */
    private boolean advanceRun(RandomAccessFile in, int runIndex, 
        int perRunBufSize, long[] runFilePtr, long[] runEndPos, int[] bufBytes,
        int[] bufNext) throws IOException {

        // checks whether the current buffer's read offset is 
        // less than the buffer's loaded bytes 
            // < means that there are more bytes/records in 
            // the buffer to be read
        if (bufNext[runIndex] < bufBytes[runIndex]) {
            return true;
        }

        // checks whether the file's reader ptr for the current 
        // run's buffer is >= to the run's buffer's end of the file ptr
            // if >=, means that the reader ptr has gotten to or 
            // surpassed the end of the run's file
        if (runFilePtr[runIndex] >= runEndPos[runIndex]) {
            return false;
        }

        // if the first checker is false and the second is true, 
        // gets to this code
            // means that it needs to move to the next record
        int toRead = (int)Math.min(perRunBufSize, runEndPos[runIndex]
            - runFilePtr[runIndex]);
        in.seek(runFilePtr[runIndex]);
        // reading the next bytes/record
        in.readFully(workingMem, runIndex * perRunBufSize, toRead);
        // updating the ptr of where the index is in the current buffer
        bufBytes[runIndex] = toRead;
        bufNext[runIndex] = 0;
        // adds more bytes to the curr buffer's reading ptr, 
        // updating to account for the bytes now read in
        runFilePtr[runIndex] += toRead;
        return true;
    }

    
    /**
     * Helper method that reads the 4-byte key for a record
     * stored at a byte offset in the working memory pool
     * by wrapping it in a ByteBuffer
     *
     * @param off
     *            byte offset in workingMem where the key starts
     * @return the integer key value at that offset
     */
    private int getKeyAt(int off) {
        return ByteBuffer.wrap(workingMem, off, 4).getInt();
    }


    /**
     * Helper method that turns the parallel key/run 
     * arrays into a min-heap to prep for multi-way merging.
     *
     * @param keys
     *            array of key values
     * @param runs
     *            array of run indices, kept in sync with keys
     * @param size
     *            number of elements in the heap
     */
    private void buildMergeHeap(int[] keys, int[] runs, int size) {
        // goes through the keys/runs pairs and organizes them into a min-heap
        for (int i = (size / 2) - 1; i >= 0; i--) {
            siftDownMerge(keys, runs, size, i);
        }
    }


    /**
     * Helper method that sifts the element at the nodes downward until 
     * the min-heap organization is in tact. Keeps the runs array in sync 
     * so that the parallel data arrays are still in the right order.
     *
     * @param keys
     *            array of key values
     * @param runs
     *            array of run indices
     * @param size
     *            current heap size
     * @param siftIndex
     *            index to start sifting from
     */
    private void siftDownMerge(int[] keys, int[] runs, int size, 
        int siftIndex) {
        
        // loops over the heap and organizes it so that min-heap logic is there
            // ensures that each internal node is smaller 
            // than both of its children
        while (siftIndex < size / 2) {
            int child = 2 * siftIndex + 1;
            if (child + 1 < size && keys[child + 1] < keys[child]) {
                child = child + 1;
            }
            if (keys[siftIndex] <= keys[child]) {
                return;
            }
            int tmpKey = keys[siftIndex];
            keys[siftIndex] = keys[child];
            keys[child] = tmpKey;

            int tmpRun = runs[siftIndex];
            runs[siftIndex] = runs[child];
            runs[child] = tmpRun;

            siftIndex = child;
        }
    }
}
