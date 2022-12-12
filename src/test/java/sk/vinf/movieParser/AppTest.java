package sk.vinf.movieParser;


import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    
    /**
     * Rigorous Test :-)
     */

    @Test
    public void testCheckTime() {
        assertTrue(10 > App.search("t:mysterious:null"));
    }

}
