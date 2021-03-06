package simulation.generator.app;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Shishir Bharathi
 */
public class AppFactory {
    private static final Map<String, Application> appMap;
    
    static {
        appMap = new HashMap<String, Application>();
        appMap.put("Ligo", new Ligo());
        appMap.put("GENOME", new Genome());
        appMap.put("MONTAGE", new Montage());
        appMap.put("Sipht", new Sipht());
        appMap.put("CYBERSHAKE", new Cybershake());
        appMap.put("VC", new VariantCalling());
    }
    
    public static Application getApp(String appName) throws Exception {
        Application app = appMap.get(appName.toUpperCase());
        if (app == null) {
            throw new Exception("Unknown application: " + appName);
        }
        return app;
    }
}
