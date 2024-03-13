package de.tum.i13.ecs;

import java.util.Comparator;

public class HashComparator implements Comparator<Node> {
    @Override
    public int compare(Node o1, Node o2) {
        return o1.get_hash_integer().compareTo(o2.get_hash_integer());
    }
}
