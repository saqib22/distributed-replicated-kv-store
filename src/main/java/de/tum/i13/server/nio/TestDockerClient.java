package de.tum.i13.server.nio;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.core.DockerClientBuilder;

import java.util.List;

public class TestDockerClient {
    public static void main(String[] args) {
        DockerClient dockerClient = DockerClientBuilder.getInstance("tcp://localhost:2375").build();
        List<Image> images = dockerClient.listImagesCmd().exec();
        images.iterator().forEachRemaining(System.out::println);

//        docker run --rm -p 39595:39595 --name ms4-gr5-ecs-server --network ms4/gr5 ms4/gr5/ecs-server
//                -a 0.0.0.0 -p 39595 -ll FINEST
        CreateContainerResponse ecs_container = dockerClient.createContainerCmd("ecs-server:latest")
                .withName("127.0.0.1")
                .withPortBindings(PortBinding.parse("5153:5153"))
                .withExposedPorts(ExposedPort.parse("5153"))
                .withNetworkMode("kv-cluster-network")
                .withCmd("-a", "0.0.0.0").exec();


        dockerClient.startContainerCmd(ecs_container.getId()).exec();

    }
}
