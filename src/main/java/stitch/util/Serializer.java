package stitch.util;

import org.apache.log4j.Logger;
import stitch.datastore.DataStoreServer;

import java.io.*;

public class Serializer {

    static final Logger logger = Logger.getLogger(DataStoreServer.class);

    public static byte[] objectToBytes(Object obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    public static Object  bytesToObject(byte[] objBytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(objBytes);
             ObjectInputStream ois = new ObjectInputStream(bis)){
            return ois.readObject();
        }
    }

    public static String bytesToString(byte[] inBytes) {
        try{
            String resourceId = new String(inBytes, "UTF-8");
            return resourceId;

        }catch(UnsupportedEncodingException error) {
            logger.error(String.format("Failed to get Resource due to unsupported encoding"),error);
        }
        return null;
    }

    private static String getCallerClassName() {
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
