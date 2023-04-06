package it.unitn.ds1;

import java.util.Objects;

public class CustomPrint {

    public static void infoPrint(String classnameFull, String type, String id, String s) {
        // get the last part of the class name
        String[] parts = classnameFull.split("\\.");
        String classname = parts[parts.length - 1].toUpperCase();
        if (!Objects.equals(id, "")){
            id = " " + id;
        }
        System.out.println("[" + type + classname + id + "][INFO] " + s);
    }


    public static void debugPrint(String classnameFull, String type, String id, String s) {
        // get the last part of the class name
        String[] parts = classnameFull.split("\\.");
        String classname = parts[parts.length - 1].toUpperCase();
        if (!Objects.equals(id, "")){
            id = " " + id;
        }
        System.out.println("[" + type + classname + id + "][DEBUG] " + s);
    }

}

