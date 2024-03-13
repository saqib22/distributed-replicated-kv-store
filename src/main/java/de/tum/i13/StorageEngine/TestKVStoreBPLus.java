package de.tum.i13.StorageEngine;

import de.tum.i13.shared.Constants;

import java.io.File;
import java.util.Iterator;

public class TestKVStoreBPLus {
    public static void main(String[] args) {
//        String directoryName = "target/" + UUID.randomUUID().toString().substring(0, 6);

        File file = new File("target/kv_pairs");
        file.mkdirs();

        BPlusTree<String, String> tree = BPlusTree.file()
                        .directory(file)
                        .maxLeafKeys(32)
                        .maxNonLeafKeys(8)
                        .uniqueKeys(true)
                        .segmentSizeMB(1)
                        .keySerializer(Serializer.utf8(Constants.KEY_SIZE_IN_BYTES))
                        .valueSerializer(Serializer.utf8(Constants.KEY_SIZE_IN_BYTES))
                        .naturalOrder();
//        // insert some values
//        tree.insert(1000, "hello");
//        tree.insert(2000, "there");
//        tree.insert(1000, "world");
//        for (int i = 0; i < 100000; i++)
//            tree.insert("ID" + i, "hello" + i);

        // search the tree for values with keys between 0 and 3000
        // and print out key value pairs
        tree.insert("EA", "hello");
        tree.insert("AB", "world");
        tree.insert("AE", "test");
//        System.out.println(tree.find("hello").iterator().next());
//        System.out.println("Getting the value now  .......");
//
//
//        Iterable result = tree.find("ID9999");
//
//        System.out.println(result.iterator().next());
//        System.out.println(result.iterator().next());
//        System.out.println(result.iterator().next());

        Iterator<Entry<String, String>> result = tree.findEntries("00", "AF").iterator();

        while(result.hasNext()){
            System.out.println(result.next().key());
        }

//        tree.findAll().forEach(System.out::println);
    }

}
