package stitch.util.properties;

import java.io.Closeable;

public interface PropertyStore extends Closeable {
    void writeKey(String keyName, String keyValue);
    String readKey(String keyName);
    String getString(String section, String propertyName);
    int getInt(String section, String propertyName);
    boolean getBool(String section, String propertyName);
    String getSecret(String sectionName, String secretName, String secretKey, String secretSalt);
    void close();
}
