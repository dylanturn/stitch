package stitch.util.configuration.store;

import org.apache.log4j.Logger;
import stitch.util.configuration.item.ConfigItem;
import stitch.util.configuration.item.ConfigItemType;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class ConfigStore {

    private static final Logger logger = Logger.getLogger(ConfigStore.class);
    private static final String propFileName = "application.properties";
    private static final  String propPrefix = "stitch_ps";

    private Properties properties = new Properties();

    public static ConfigStore loadConfigStore() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class propClass = Class.forName(loadApplicationProperties().getProperty(String.format("%s_class", propPrefix)));
        return (ConfigStore) propClass.newInstance();
    }

    private static Properties loadApplicationProperties(){
        InputStream inputStream = ConfigStore.class.getClassLoader().getResourceAsStream(propFileName);
        if (inputStream != null) {
            try {
                Properties properties = new Properties();
                properties.load(inputStream);
                return properties;
            } catch (IOException e) {
                logger.error("Failed to load application configuration input stream", e);
                e.printStackTrace();
                System.exit(100);
            }
        }
        return null;
    }

    public ConfigStore(){
        this.properties = loadApplicationProperties();
    }

    protected String readProperty(String propertyName){

        return properties.getProperty(String.format("%s_%s", propPrefix, propertyName));
    }

    public abstract void writeKey(String keyName, String keyValue);
    public abstract String readKey(String keyName);
    public abstract String getString(String section, String propertyName);
    public abstract int getInt(String section, String propertyName);
    public abstract boolean getBool(String section, String propertyName);
    public abstract String getSecret(String sectionName, String secretName, String secretKey, String secretSalt);

    public abstract ConfigItem getConfigItemById(String configItemId);
    public abstract ConfigItem getConfigItemByName(ConfigItemType configItemType);
    public abstract List<ConfigItem> listConfigByItemType(ConfigItemType configItemType);
    public abstract List<ConfigItem> getConfigItemsByAttribute(String attributeName, String attributeValue);
    public abstract List<ConfigItem> getConfigItemsByAttributes(Map<String,String> attributes);
}
