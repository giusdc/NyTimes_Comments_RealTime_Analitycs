package kafkastream.utils;

import java.io.*;

public class SerializerUtils {
    public static byte[] serialize(Object object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {

            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(object);
            oos.flush();
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bos.toByteArray();
    }

    public static Object deserialize(byte[] bytes) {
        ObjectInputStream is = null;
        Object object = null;
        try {
            is = new ObjectInputStream(new ByteArrayInputStream(bytes));
            object = is.readObject();
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return object;
    }
}
