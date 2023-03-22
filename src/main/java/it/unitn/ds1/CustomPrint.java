package it.unitn.ds1;

public class CustomPrint {

    public static void print(String classnameFull, String s) {
        // get the last part of the class name
        String[] parts = classnameFull.split("\\.");
        String classname = parts[parts.length - 1];
        System.out.println("[" + classname + "] " + s);
    }


    public static void debugPrint(String classnameFull, String s) {

        // get the last part of the class name
        String[] parts = classnameFull.split("\\.");
        String classname = parts[parts.length - 1];

        System.out.println("[" + classname + "][DEBUG] " + s);
    }

}

