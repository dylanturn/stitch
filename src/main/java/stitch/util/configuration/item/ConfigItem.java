package stitch.util.configuration.item;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigItem {

    private Map<String, Object> objectProps = new HashMap<>();

    public String getConfigId() {
        return getConfigString("uuid");
    }

    public String getConfigName() {
        return getConfigString("name");
    }

    public ConfigItemType getConfigType(){
        return ConfigItemType.valueOf(getConfigString("type").toUpperCase());
    }

    public Object getConfigObject(String key) {
        return objectProps.get(key);

    }

    public String getConfigString(String key) {
        return String.valueOf(getConfigObject(key));
    }

    public String getConfigSecret(String key, String secretKey, String secretSalt){
        // TODO: Actually implement decryption.
        return getConfigString(key);
    }

    public int getConfigInt(String key) {
        return Integer.parseInt(getConfigString(key));
    }

    public Class getConfigClass(String key) throws ClassNotFoundException {
        return Class.forName(getConfigString(key));
    }

    public long getConfigLong(String key) {
        return Long.parseLong(getConfigString(key));
    }

    public boolean getConfigBool(String key){
        return Boolean.parseBoolean(getConfigString(key));
    }

    public ConfigItem setConfigId(String objectId){
        this.objectProps.put("uuid", objectId);
        return this;
    }

    public ConfigItem setConfigName(String objectName){
        this.objectProps.put("name", objectName);
        return this;
    }

    public ConfigItem setObjectType(ConfigItemType configItemType){
        this.setObjectType(configItemType.toString());
        return this;
    }

    public ConfigItem setObjectType(String configItemType){
        this.objectProps.put("type", configItemType);
        return this;
    }

    public ConfigItem addConfigProperties(String key, Object value){
        this.objectProps.put(key, value);
        return this;
    }

    public ConfigItem addAllConfigProperties(Map values){
        this.objectProps.putAll(values);
        return this;
    }

    public ConfigItem addAllConfigsByKeys(List<String> keylist, Map<String, Object> objectMap) {
        this.addAllConfigsByKeys(keylist.toArray(new String[0]), objectMap);
        return this;
    }

    public ConfigItem addAllConfigsByKeys(String[] keylist, Map<String, Object> objectMap){
        for(String key : keylist){
            this.addConfigProperties(key, objectMap.get(key));
        }
        return this;
    }

}
