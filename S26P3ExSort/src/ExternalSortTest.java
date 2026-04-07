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
        
        assertTrue(systemOut().getHistory()
            .contains("Usage: ExernalSortProj <data-file-name>"));
        
    }
    
    /**
     * Testing when the setup has no file
     */
    public void testNoFile() throws Exception {
        String[] arguments = {"fakeFile.bin"};
        ExternalSortProj.main(arguments);
        
        assertTrue(systemOut().getHistory()
            .contains("There is no such input file as |fakeFile.bin|"));
        
    }
}
