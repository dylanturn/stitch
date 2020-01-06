package stitch.util;

public class BaseObject {
    private String prefix;
    private String id;

    public BaseObject(String prefix, String id){
        this.prefix = prefix;
        this.id = id;
    }

    public String getPrefix(){
        return this.prefix;
    }
    public String getId(){
        return this.id;
    }
    public String getRouteKey(){
        return String.format("%s_%s", this.getPrefix(), this.getId());
    }
}
