package stitch.util.properties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StitchProperty {
    private String objectId;
    private String objectName;
    private StitchType objectType;
    private Class objectClass;
    private Map<String, Object> objectProps = new HashMap<>();


    public String getObjectId() {
        return objectId;
    }
    public String getObjectName() {
        return objectName;
    }
    public StitchType getObjectType(){
        return objectType;
    }
    public Class getObjectClass(){
        return objectClass;
    }
    public Object getPropertyObject(String key) {
        return objectProps.get(key);
    }
    public String getPropertyString(String key) {
        return String.valueOf(getPropertyObject(key));
    }
    public int getPropertyInt(String key) {
        return Integer.parseInt(getPropertyString(key));
    }
    public boolean getPropertyBool(String key){
        return Boolean.parseBoolean(getPropertyString(key));
    }

    public StitchProperty setObjectId(String objectId){
        this.objectId = objectId;
        return this;
    }
    public StitchProperty setObjectName(String objectName){
        this.objectName = objectName;
        return this;
    }
    public StitchProperty setObjectType(StitchType stitchType){
        this.objectType = stitchType;
        return this;
    }
    public StitchProperty setObjectType(String stitchType){
        this.objectType = StitchType.valueOf(stitchType);
        return this;
    }
    public StitchProperty setObjectClass(Class objectClass) throws ClassNotFoundException{
        this.objectClass = objectClass;
        return this;
    }
    public StitchProperty setObjectClass(String objectClass) throws ClassNotFoundException{
        this.objectClass = Class.forName(objectClass);
        return this;
    }
    public StitchProperty addObjectProperty(String key, Object value){
        this.objectProps.put(key, value);
        return this;
    }
    public StitchProperty addAllObjectProperty(Map values){
        this.objectProps.putAll(values);
        return this;
    }
    public StitchProperty addAllObjectsByKeys(List<String> keylist, Map<String, Object> objectMap) {
        this.addAllObjectsByKeys(keylist.toArray(new String[0]), objectMap);
        return this;
    }
    public StitchProperty addAllObjectsByKeys(String[] keylist, Map<String, Object> objectMap){
        for(String key : keylist){
            this.addObjectProperty(key, objectMap.get(key));
        }
        return this;
    }


}
