import java.nio.*;
import java.io.*;

/**
 * @author Brianna McDonald, Tim Pactwa
 * @version Spring 2026
 */
public class ExternalSort {

    private static final int MEMBYTES = 50000;
    private static final int RECORD_SIZE = 8;
    private static final int BLOCK_SIZE = 4096;
    private byte[] workingMem = new byte[MEMBYTES];

    /**
     * Sorts the binary file at the given path using external sort.
     * Splits the file into sorted runs, then merges them back together.
     *
     * @param theFileName
     *            The name of the file to be sorted
     * @throws IOException
     */
    public static void sort(String theFileName) throws IOException {
        ExternalSort sorter = new ExternalSort();
        File f = new File(theFileName);
        long fileSize = f.length();

        int outputBufStart = MEMBYTES - BLOCK_SIZE;
        int heapAreaSize = outputBufStart;
        int heapRecordCap = heapAreaSize / RECORD_SIZE;
        int heapBytesCap = heapRecordCap * RECORD_SIZE;

        RandomAccessFile theFile = new RandomAccessFile(theFileName, "rw");
        RandomAccessFile tempFile = new RandomAccessFile("tempSortFile.bin",
            "rw");
        tempFile.setLength(0);

        int numRuns = 0;
        long bytesRead = 0;

        while (bytesRead < fileSize) {
            int bytesToRead = (int)Math.min(heapBytesCap, fileSize - bytesRead);
            theFile.seek(bytesRead);
            theFile.readFully(sorter.workingMem, 0, bytesToRead);

            int numRecs = bytesToRead / RECORD_SIZE;
            MinHeap heap = new MinHeap(sorter.workingMem, 0, numRecs, numRecs);

            sorter.extractAndWrite(heap, tempFile, bytesRead, numRecs,
                outputBufStart);

            bytesRead += bytesToRead;
            numRuns++;
        }

        if (numRuns > 1) {
            sorter.mergeRuns(tempFile, theFile, numRuns, fileSize,
                heapBytesCap);
        }
        else {
            // only one run, just copy temp back to the original file
            theFile.seek(0);
            tempFile.seek(0);
            long remaining = fileSize;
            while (remaining > 0) {
                int toRead = (int)Math.min(BLOCK_SIZE, remaining);
                tempFile.readFully(sorter.workingMem, 0, toRead);
                theFile.write(sorter.workingMem, 0, toRead);
                remaining -= toRead;
            }
        }

        theFile.close();
        tempFile.close();
        new File("tempSortFile.bin").delete();
    }


    /**
     * Pulls sorted records out of the heap one by one and writes
     * them to the output file through a block-sized buffer.
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
    private void extractAndWrite(
        MinHeap heap,
        RandomAccessFile outFile,
        long fileOffset,
        int numRecs,
        int outputBufStart)
        throws IOException {

        outFile.seek(fileOffset);
        int bufferIndex = 0;

        for (int i = 0; i < numRecs; i++) {
            int srcOffset = heap.removeMinOffset();
            System.arraycopy(workingMem, srcOffset, workingMem, outputBufStart
                + bufferIndex, RECORD_SIZE);
            bufferIndex += RECORD_SIZE;

            if (bufferIndex >= BLOCK_SIZE) {
                outFile.write(workingMem, outputBufStart, bufferIndex);
                bufferIndex = 0;
            }
        }

        if (bufferIndex > 0) {
            outFile.write(workingMem, outputBufStart, bufferIndex);
        }
    }


    /**
     * Merges all sorted runs into one sorted output using a k-way merge.
     * Keeps a small heap of the current front record from each run so
     * we can always grab the global minimum in O(log k) time.
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
    private void mergeRuns(
        RandomAccessFile in,
        RandomAccessFile outFile,
        int numRuns,
        long totalBytes,
        int maxRunSize)
        throws IOException {

// split memory evenly among all run buffers plus one output buffer
        int perRunBufSize = MEMBYTES / (numRuns + 1);
        perRunBufSize = (perRunBufSize / RECORD_SIZE) * RECORD_SIZE;
        if (perRunBufSize < RECORD_SIZE) {
            perRunBufSize = RECORD_SIZE;
        }

        int outputBufStart = numRuns * perRunBufSize;
        int outputBufSize = MEMBYTES - outputBufStart;
        int outputBufIndex = 0;

        long[] runFilePtr = new long[numRuns];
        long[] runEnd = new long[numRuns];
        int[] bufBytes = new int[numRuns];
        int[] bufNext = new int[numRuns];

        for (int i = 0; i < numRuns; i++) {
            runFilePtr[i] = (long)i * maxRunSize;
            runEnd[i] = Math.min(runFilePtr[i] + maxRunSize, totalBytes);
            bufBytes[i] = 0;
            bufNext[i] = 0;
        }

        // load the first chunk of each run into memory
        int activeRuns = 0;
        for (int i = 0; i < numRuns; i++) {
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

        // seed the merge heap with the first key from each run
        int heapSize = activeRuns;
        int[] mergeKeys = new int[heapSize];
        int[] mergeRun = new int[heapSize];

        int idx = 0;
        for (int i = 0; i < numRuns; i++) {
            if (bufBytes[i] > 0) {
                int off = (i * perRunBufSize) + bufNext[i];
                mergeKeys[idx] = getKeyAt(off);
                mergeRun[idx] = i;
                idx++;
            }
        }

        buildMergeHeap(mergeKeys, mergeRun, heapSize);

        outFile.seek(0);

        while (heapSize > 0) {
            int minRunIdx = mergeRun[0];

            // copy the minimum record into the output buffer
            int srcOff = (minRunIdx * perRunBufSize) + bufNext[minRunIdx];
            System.arraycopy(workingMem, srcOff, workingMem, outputBufStart
                + outputBufIndex, RECORD_SIZE);
            outputBufIndex += RECORD_SIZE;
            bufNext[minRunIdx] += RECORD_SIZE;

            if (outputBufIndex >= outputBufSize) {
                outFile.write(workingMem, outputBufStart, outputBufIndex);
                outputBufIndex = 0;
            }

            boolean hasNext = advanceRun(in, minRunIdx, perRunBufSize,
                runFilePtr, runEnd, bufBytes, bufNext);

            if (hasNext) {
                // put the next record from this run at the top of the heap
                int nextOff = (minRunIdx * perRunBufSize) + bufNext[minRunIdx];
                mergeKeys[0] = getKeyAt(nextOff);
                siftDownMerge(mergeKeys, mergeRun, heapSize, 0);
            }
            else {
                // this run is done, pull the last heap entry up to 
                // fill the gap
                heapSize--;
                if (heapSize > 0) {
                    mergeKeys[0] = mergeKeys[heapSize];
                    mergeRun[0] = mergeRun[heapSize];
                    siftDownMerge(mergeKeys, mergeRun, heapSize, 0);
                }
            }
        }

        if (outputBufIndex > 0) {
            outFile.write(workingMem, outputBufStart, outputBufIndex);
        }
    }


    /**
     * Moves to the next record in the given run's buffer.
     * If the buffer is empty, reads the next chunk from disk.
     * Returns false if the run has no more records left.
     *
     * @param in
     *            the input file to read from
     * @param runIdx
     *            which run to advance
     * @param perRunBufSize
     *            size of each run's in-memory buffer
     * @param runFilePtr
     *            current read position in the file for each run
     * @param runEnd
     *            end position in the file for each run
     * @param bufBytes
     *            how many bytes are loaded in each run's buffer
     * @param bufNext
     *            current read offset within each run's buffer
     * @return true if a next record is available, false if the run is done
     * @throws IOException
     *             if file I/O fails
     */
    private boolean advanceRun(
        RandomAccessFile in,
        int runIdx,
        int perRunBufSize,
        long[] runFilePtr,
        long[] runEnd,
        int[] bufBytes,
        int[] bufNext)
        throws IOException {

        if (bufNext[runIdx] < bufBytes[runIdx]) {
            return true;
        }

        if (runFilePtr[runIdx] >= runEnd[runIdx]) {
            return false;
        }

        int toRead = (int)Math.min(perRunBufSize, runEnd[runIdx]
            - runFilePtr[runIdx]);
        in.seek(runFilePtr[runIdx]);
        in.readFully(workingMem, runIdx * perRunBufSize, toRead);
        bufBytes[runIdx] = toRead;
        bufNext[runIdx] = 0;
        runFilePtr[runIdx] += toRead;
        return true;
    }


    /**
     * Reads the 4-byte key stored at a given offset in working memory.
     *
     * @param off
     *            byte offset in workingMem
     * @return the integer key value
     */
    private int getKeyAt(int off) {
        return ((workingMem[off] & 0xFF) << 24) | ((workingMem[off + 1]
            & 0xFF) << 16) | ((workingMem[off + 2] & 0xFF) << 8)
            | (workingMem[off + 3] & 0xFF);
    }


    /**
     * Turns the parallel key/run arrays into a valid min-heap.
     *
     * @param keys
     *            array of key values
     * @param runs
     *            array of run indices, kept in sync with keys
     * @param size
     *            number of elements in the heap
     */
    private void buildMergeHeap(int[] keys, int[] runs, int size) {
        for (int i = (size / 2) - 1; i >= 0; i--) {
            siftDownMerge(keys, runs, size, i);
        }
    }


    /**
     * Sifts the element at pos downward until the min-heap
     * property is restored. Keeps the runs array in sync.
     *
     * @param keys
     *            array of key values
     * @param runs
     *            array of run indices
     * @param size
     *            current heap size
     * @param pos
     *            index to start sifting from
     */
    private void siftDownMerge(int[] keys, int[] runs, int size, int pos) {
        while (pos < size / 2) {
            int child = 2 * pos + 1;
            if (child + 1 < size && keys[child + 1] < keys[child]) {
                child = child + 1;
            }
            if (keys[pos] <= keys[child]) {
                return;
            }
            int tmpKey = keys[pos];
            keys[pos] = keys[child];
            keys[child] = tmpKey;

            int tmpRun = runs[pos];
            runs[pos] = runs[child];
            runs[child] = tmpRun;

            pos = child;
        }
    }
}
