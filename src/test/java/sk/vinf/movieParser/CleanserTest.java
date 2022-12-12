package sk.vinf.movieParser;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.json.JSONObject;

public class CleanserTest {

    private Cleanser clnsr = new Cleanser();

    @Test
    public void testCleanId() {
        JSONObject json = new JSONObject();
        String predicate = "<http://rdf.freebase.com/ns/g.11b6t1scwz>";
        json.put("subject", predicate);
        assertEquals("g.11b6t1scwz", clnsr.cleanId(json));
    }
    
}
