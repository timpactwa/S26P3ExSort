import java.nio.ByteBuffer;
import java.util.Arrays;
import student.TestCase;

/**
 * This class was designed to test the External Sort class.
 * Each tests generates random ascii and binary files of the specified size,
 * then sorts both and then checking each one with the file checker.
 *
 * @author CS3114/5040 Staff
 * @version Spring 2026
 */
public class ExternalSortTest extends TestCase {
    private CheckFile fileChecker;

    /**
     * This method sets up the tests that follow.
     */
    public void setUp() {
        fileChecker = new CheckFile();
    }


    // ----------------------------------------------------------
    /**
     * Helper method for the tests: Run a test suite for a given size.
     * Creates two files (one "ascii" and one "binary") of the specified size,
     * then for each one, runs the sort and runs the checker.
     * 
     * @param fileSize
     *            Number of (4096 byte) blocks to test for
     * @throws Exception
     */
    public void sortHelper(int fileSize) throws Exception {

        FileGenerator it = new FileGenerator();
        String namea = "input" + fileSize + "asave.bin";
        String nameb = "input" + fileSize + "bsave.bin";
        it.generateFile(namea, fileSize, "a");
        it.generateFile(nameb, fileSize, "b");
        String[] args = new String[1];

        String testFilea = "testa" + fileSize + ".bin";
        args[0] = testFilea;
        SortUtils.copyFile(namea, testFilea);
        System.out.println("Sorting " + testFilea);
        ExternalSortProj.main(args);
        assertTrue(fileChecker.checkFileA(testFilea, fileSize));

        String testFileb = "testb" + fileSize + ".bin";
        args[0] = testFileb;
        SortUtils.copyFile(nameb, testFileb);
        System.out.println("Sorting " + testFileb);
        ExternalSortProj.main(args);
        assertTrue(fileChecker.checkFile(testFileb, fileSize));
    }


    // ----------------------------------------------------------
    /**
     * Test a file with 1 block
     * 
     * @throws Exception
     */
    public void test1() throws Exception {
        sortHelper(1);
    }


    /**
     * Test a file with 2 blocks
     * 
     * @throws Exception
     */
    public void test2() throws Exception {
        sortHelper(2);
    }


    /**
     * Test a file with 10 blocks
     * 
     * @throws Exception
     */
    public void test10() throws Exception {
        sortHelper(10);
    }


    /**
     * Test a file with 100 blocks
     * 
     * @throws Exception
     */
    public void test100() throws Exception {
        sortHelper(100);
    }


    /**
     * Test a file with 101 blocks
     * 
     * @throws Exception
     */
    public void test101() throws Exception {
        sortHelper(101);
    }


    /**
     * Test a file with 130 blocks
     * 
     * @throws Exception
     */
    public void test130() throws Exception {
        sortHelper(130);
    }


    /**
     * Test a file with 200 blocks
     * 
     * @throws Exception
     */
    public void test200() throws Exception {
        sortHelper(200);
    }


    /**
     * Test a file with 1001 blocks
     * 
     * @throws Exception
     */
    public void test1001() throws Exception {
        sortHelper(1001);
    }


    /**
     * Test a file with 2048 blocks
     * 
     * @throws Exception
     */
    public void test2048() throws Exception {
        sortHelper(2048);
    }


    /**
     * Testing when the setup has no file name for the args
     */
    public void testArguments() throws Exception {
        String[] arguments = new String[0];
        ExternalSortProj.main(arguments);

        assertTrue(systemOut().getHistory().contains(
            "Usage: ExernalSortProj <data-file-name>"));

    }


    /**
     * Testing when the setup has no file
     */
    public void testNoFile() throws Exception {
        String[] arguments = { "fakeFile.bin" };
        ExternalSortProj.main(arguments);

        assertTrue(systemOut().getHistory().contains(
            "There is no such input file as |fakeFile.bin|"));

    }


    /**
     * Testing records
     */
    public void testRecords() {
        byte[] bytes = new byte[8];
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        int key = 4444;
        int val = 7777;
        buff.putInt(key);
        buff.putInt(val);

        Record rec = new Record(bytes);
        assertEquals("Key should be 4444", key, rec.getKey());
        assertEquals("Value should be 7777", val, rec.getValue());
    }


    /**
     * Testing records
     */
    public void testRecords2() {
        byte[] bytes = new byte[8];
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        int key = 4444;
        int val = 7777;
        buff.putInt(key);
        buff.putInt(val);

        Record rec = new Record(bytes);
        byte[] expected = rec.toBytes();

        assertEquals("Byte array length should be 8", 8, expected.length);
        assertTrue("The output bytes should match the original bytes", Arrays
            .equals(bytes, expected));

        assertEquals("Key should be 4444", key, rec.getKey());
        assertEquals("Value should be 7777", val, rec.getValue());
    }


    /**
     * Testing heap
     */
    public void testHeap1() {
        byte[] pool = new byte[40];
        MinHeap heap = new MinHeap(pool, 0, 0, 5);
        assertEquals("Heap size should initially be 0", 0, heap.heapSize());

        byte[] recBytes = new byte[8];
        Record rec = new Record(recBytes);
        byte[] recBytes2 = new byte[8];
        Record rec2 = new Record(recBytes2);

        heap.insert(rec);
        assertEquals("Heap size should be 1 after one insertion", 1, heap
            .heapSize());
        heap.insert(rec2);
        assertEquals("Heap size should be 2 after second insertion", 2, heap
            .heapSize());

        heap.removeMin();
        assertEquals("Heap size should be 1 after removing the minimum", 1, heap
            .heapSize());

        MinHeap heap2 = new MinHeap(pool, 0, 3, 5);
        assertEquals(
            "Heap size should match the initial size passed to constructor", 3,
            heap2.heapSize());
    }
}
