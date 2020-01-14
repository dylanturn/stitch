package stitch.util;

import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;

public class Serializer {

    public static String bytesToString(byte[] inBytes){

        Logger logger = Logger.getLogger(getCallerCallerClassName());

        try{
            String resourceId = new String(inBytes, "UTF-8");
            return resourceId;
        }catch(UnsupportedEncodingException error) {
            logger.error(String.format("Failed to get Resource due to unsupported encoding"),error);
            return null;
        }
    }

    private static String getCallerCallerClassName() {
        StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
        String callerClassName = null;
        for (int i=1; i<stElements.length; i++) {
            StackTraceElement ste = stElements[i];
            if (!ste.getClassName().equals(Serializer.class.getName())&& ste.getClassName().indexOf("java.lang.Thread")!=0) {
                if (callerClassName==null) {
                    callerClassName = ste.getClassName();
                } else if (!callerClassName.equals(ste.getClassName())) {
                    return ste.getClassName();
                }
            }
        }
        return null;
    }
}
