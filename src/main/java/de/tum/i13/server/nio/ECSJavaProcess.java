package de.tum.i13.server.nio;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public final class ECSJavaProcess {

    private ECSJavaProcess() {}

    public static int exec(Class klass, List<String> args) throws IOException,
            InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = klass.getName();

        List<String> command = new LinkedList<String>();
        command.add(javaBin);
        command.add("-cp");
        command.add(classpath);
        command.add(className);
        if (args != null) {
            command.addAll(args);
        }

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        builder.redirectOutput(new File("echo.log"));
        Process process = builder.inheritIO().start();
        return 0;
    }

}