import java.nio.*;
import java.io.*;

// The External Sort implementation
// -------------------------------------------------------------------------
/**
 * @author {Your Name Here}
 * @version Spring 2026
 */
public class ExternalSort {

    /**
     * The working memory available to the program: 50,000 bytes
     */
    private static final int MEMBYTES = 50000;

    // size of 1 singular disk block in bytes amt
    private static final int BLOCK_SIZE = 4096;

    // each record has a size of 8 bytes (4 for key and 4 for data)
    private static final int RECORD_SIZE = 8;

    // amt of records that can be max in any singular disk block
    private static final int RECORDS_PER_BLK = BLOCK_SIZE / RECORD_SIZE;

    // Allocate 50,000 bytes of working memory
    private byte[] workingMem = new byte[MEMBYTES];

    // num records currently in the memory
    private int numRecords = 0;

    /**
     * Create a new ExternalSort object.
     * 
     * @param theFileName
     *            The name of the file to be sorted
     * @throws IOException
     */
    public static void sort(String theFileName) throws IOException {
        ExternalSort sorter = new ExternalSort();
        RandomAccessFile theFile = new RandomAccessFile(theFileName, "rw");
        RandomAccessFile tempFile = new RandomAccessFile("TempFile", "rw");

        int heapCapacity = MEMBYTES - BLOCK_SIZE;
        int heapMaxRecords = heapCapacity / RECORD_SIZE;
        int outputBufferOffset = heapCapacity;

        theFile.seek(0);

        int numRuns = 0;
        int maxRuns = (int) (theFile.length() / heapCapacity) + 2;
        int[] runLengths = new int[maxRuns];

        while (theFile.getFilePointer() < theFile.length()) {

            // fills the heap section of the working memory
            // from the input file
            int bytesRead = 0;
            while (bytesRead < heapCapacity && theFile
                .getFilePointer() < theFile.length()) {
                int toRead = Math.min(BLOCK_SIZE, heapCapacity - bytesRead);
                theFile.read(sorter.workingMem, bytesRead, toRead);
                bytesRead += toRead;
            }
            int numRecsLoaded = bytesRead / RECORD_SIZE;

            // goes thru the records from heap section and puts into array
            Record[] records = new Record[numRecsLoaded];
            ByteBuffer heapBuf = ByteBuffer.wrap(sorter.workingMem, 0,
                bytesRead);
            for (int i = 0; i < numRecsLoaded; i++) {
                byte[] record = new byte[RECORD_SIZE];
                heapBuf.get(record);
                records[i] = new Record(record);
            }

            // makes the minheap with the block of current records
            MinHeap heap = new MinHeap(records, numRecsLoaded, numRecsLoaded);

            // takes all the mins from the heap and loads them
            // into the output buffer and will push to the output
            // file and reset buffer when it gets full
            // (512 records in the buffer)
            ByteBuffer outBuffer = ByteBuffer.wrap(sorter.workingMem,
                outputBufferOffset, BLOCK_SIZE);
            int runBytes = 0;
            while (heap.heapSize() > 0) {
                Record min = heap.removeMin();
                outBuffer.putInt(min.getKey());
                outBuffer.putInt(min.getValue());

                // output buff is full, moves records into the
                // output file and clears
                if (outBuffer.remaining() == 0) {
                    tempFile.write(sorter.workingMem, outputBufferOffset,
                        BLOCK_SIZE);
                    outBuffer.clear();
                    runBytes += BLOCK_SIZE;
                }
            }

            // gets rid of any leftover records in the output
            // buffer and puts into the output file
            if (outBuffer.position() > 0) {
                tempFile.write(sorter.workingMem, outputBufferOffset, outBuffer
                    .position());
                runBytes += outBuffer.position();
                outBuffer.clear();
            }

            runLengths[numRuns] = runBytes;
            numRuns++;
        }

        // one run writing into the output file
        if (numRuns == 1) {
            tempFile.seek(0);
            theFile.seek(0);

            int remaining = runLengths[0];
            int copied = 0;
            while (copied < remaining) {
                int toRead = Math.min(BLOCK_SIZE, remaining - copied);
                tempFile.read(sorter.workingMem, 0, toRead);
                theFile.write(sorter.workingMem, 0, toRead);
                copied += toRead;
            }
        }
        
        // multiple runs of sort occurred
        // need to combine since there is more than one buffer
        else {
            
            // setting up and filling in the bufferOnset array
            int[] bufferOffset = new int[numRuns];
            for (int i = 0; i < numRuns; i++) {
                // each run has to be 4096 bytes because the specs
                // say that the input file has a byte size of a
                // multiple of 4096
                bufferOffset[i] = i * BLOCK_SIZE;
            }
            int outputBufOffset = numRuns * BLOCK_SIZE;

            // initializing parallel arrays needed to merge the
            // runs together so that all the data for the runs
            // are kept in order
            int[] bufPos = new int[numRuns]; // current record index within
                                             // buffer
            int[] recsInBuf = new int[numRuns]; // records currently loaded in
                                                // each
                                                // run's buffer
            int[] runRemaining = new int[numRuns]; // bytes left to read from
                                                   // disk
                                                   // for each run
            int[] runFilePosition = new int[numRuns]; // read position in temp
                                                      // file
            int activeRuns = numRuns;

            int offset = 0;
            for (int i = 0; i < numRuns; i++) {
                runFilePosition[i] = offset;
                runRemaining[i] = runLengths[i];
                offset += runLengths[i];
            }

            // load first block from each run
            for (int i = 0; i < numRuns; i++) {
                int toRead = Math.min(BLOCK_SIZE, runRemaining[i]);
                tempFile.seek(runFilePosition[i]);
                tempFile.read(sorter.workingMem, bufferOffset[i], toRead);
                // updating run file pos
                runFilePosition[i] += toRead;
                // decrementing amt of bytes left for the currect run
                runRemaining[i] -= toRead;
                recsInBuf[i] = toRead / RECORD_SIZE;
                bufPos[i] = 0;
            }

            ByteBuffer outBuffer = ByteBuffer.wrap(sorter.workingMem,
                outputBufOffset, BLOCK_SIZE);
            theFile.seek(0);

            // iterates through the active runs finding the subsequent min
            // values in every buffer to add to the output file
            while (activeRuns > 0) {
                int tempMin = Integer.MAX_VALUE;
                int minBuffer = -1;

                // tracking which runs still need to be used
                for (int i = 0; i < numRuns; i++) {
                    if (recsInBuf[i] == 0)
                        continue;
                    // run is active, find the minimum in this run
                    // making it so we can access the key and val of the
                    // specific
                    // buffer's data
                    int recordOffset = bufferOffset[i] + (bufPos[i]
                        * RECORD_SIZE);
                    ByteBuffer bb = ByteBuffer.wrap(sorter.workingMem,
                        recordOffset, RECORD_SIZE);
                    int key = bb.getInt();
                    int val = bb.getInt();

                    // checking whether current buffer's smallest
                    // record is smaller than tempMin variable
                    if (key < tempMin) {
                        tempMin = key;
                        minBuffer = i; // keeping track of which buffer has the
                                       // smallest record
                    }
                    // if not smaller, will skip this buffer and go to the next
                    // active buffer
                }
                if (minBuffer == -1)
                    break; // all runs are completely used

                // write current min to output buffer
                int minOffset = bufferOffset[minBuffer] + (bufPos[minBuffer]
                    * RECORD_SIZE);
                outBuffer.put(sorter.workingMem, minOffset, RECORD_SIZE);
                bufPos[minBuffer]++;

                // flush buffer if it's full
                if (outBuffer.remaining() == 0) {
                    theFile.write(sorter.workingMem, outputBufOffset,
                        BLOCK_SIZE);
                    outBuffer.clear();
                }

                // refill the curr run's buffer if it got used
                if (bufPos[minBuffer] >= recsInBuf[minBuffer]) {
                    if (runRemaining[minBuffer] > 0) {
                        int toRead = Math.min(BLOCK_SIZE,
                            runRemaining[minBuffer]);
                        tempFile.seek(runFilePosition[minBuffer]);
                        tempFile.read(sorter.workingMem,
                            bufferOffset[minBuffer], toRead);
                        runFilePosition[minBuffer] += toRead;
                        runRemaining[minBuffer] -= toRead;
                        recsInBuf[minBuffer] = toRead / RECORD_SIZE;
                        bufPos[minBuffer] = 0;
                    }
                    else {
                        recsInBuf[minBuffer] = 0;
                        activeRuns--;
                    }
                }
            }

            // precaution to flush again in case there are
            // more records in the output buffer
            if (outBuffer.position() > 0) {
                theFile.write(sorter.workingMem, outputBufOffset, outBuffer
                    .position());
                outBuffer.clear();
            }
        }

        theFile.close();
        tempFile.close();
    }


    /**
     * Reads theFile file and puts it into the memory pool.
     * 
     * @param file
     *            is the file that is getting read
     * @throws IOException
     *             when anything fails to be read
     */
    public void readFile(RandomAccessFile file) throws IOException {
        // reset file pointer
        file.seek(0);

        int totalRead = 0;

        // read up to either the length of the file or the max memory pool
        int fileSize = (int)file.length();
        int toRead = Math.min(fileSize, MEMBYTES);

        // make sure to read entire file past first 2048 bytes
        while (totalRead < toRead) {
            int readBytes = file.read(workingMem, totalRead, toRead
                - totalRead);

            // nothing found case
            if (readBytes < 0)
                break;

            totalRead += readBytes;
        }

        numRecords = totalRead / RECORD_SIZE;
    }


    /**
     * Writes the memory pool to the file by one block each.
     * 
     * @param file
     *            is the file to write into
     * @throws IOException
     *             when anything fails to be written
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
