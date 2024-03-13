package de.tum.i13.server.nio;

import de.tum.i13.ecs.Main;

import java.io.IOException;
import java.util.Arrays;

public class TestECSProcessStartup {
    public static void main(String[] args) throws IOException, InterruptedException {
        int status = ECSJavaProcess.exec(Main.class, Arrays.asList("-ll","OFF"));
        System.out.println("The ECS is started");
    }
}
