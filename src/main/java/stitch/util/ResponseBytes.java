package stitch.util;

public class ResponseBytes {

    private static byte[] response = new byte[1];

    public static byte[] OK(){
        response[0] = 50;
        return response;
    }
    public static byte[] NULL(){
        response[0] = 60;
        return response;
    }
    public static byte[] ERROR(){
        response[0] = 70;
        return response;
    }
}
