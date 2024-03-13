package de.tum.i13.server.kv;

import de.tum.i13.StorageEngine.BPlusTree;
import de.tum.i13.StorageEngine.Entry;
import de.tum.i13.StorageEngine.Serializer;
import de.tum.i13.shared.Constants;

import java.io.File;
import java.util.Iterator;
import java.util.Objects;

public class KVDiskStorage implements KVStore {
    private File file;
    private BPlusTree<String, String> tree;

    public KVDiskStorage(){
        file = new File(Constants.STORAGE_DIRECTORY_NAME);
        file.mkdirs();
        tree = BPlusTree.file()
                .directory(file)
                .maxLeafKeys(32)
                .maxNonLeafKeys(8)
                .uniqueKeys(true)
                .segmentSizeMB(10)
                .keySerializer(Serializer.utf8(Constants.KEY_SIZE_IN_BYTES))
                .valueSerializer(Serializer.utf8(Constants.KEY_SIZE_IN_BYTES))
                .naturalOrder();
    }


    @Override
    public KVMessage put(String key, String value) throws Exception {
        try {
            KVMessage query_storage = this.get(key);

            if(value.equals("null") && query_storage.getStatus() == KVMessage.StatusType.GET_SUCCESS){
                tree.insert(key, value);
                return new KVResponse(key, value, KVMessage.StatusType.DELETE_SUCCESS);
            } else if (value.equals("null") && query_storage.getStatus() == KVMessage.StatusType.GET_ERROR) {
                return new KVResponse(key, value, KVMessage.StatusType.DELETE_ERROR);
            }

            if (query_storage.getStatus() == KVMessage.StatusType.GET_SUCCESS){
                tree.insert(key, value);
                return new KVResponse(key, value, KVMessage.StatusType.PUT_UPDATE);
            }
            tree.insert(key, value);
            return new KVResponse(key, value, KVMessage.StatusType.PUT_SUCCESS);
        } catch (Exception e){
            return new KVResponse(key, value, KVMessage.StatusType.PUT_ERROR);
        }

    }
    @Override
    public KVMessage get(String key) throws Exception {
        Iterator query_result = tree.find(key).iterator();
        if (query_result.hasNext()){
            String value = query_result.next().toString();
            if (!Objects.equals(value, "null"))
                return new KVResponse(key, value, KVMessage.StatusType.GET_SUCCESS);
        }
        return new KVResponse(key, null, KVMessage.StatusType.GET_ERROR);
    }
    @Override
    public Iterator<Entry<String, String>> get_entries(String r_min, String r_max){
        return this.tree.findEntries(r_min, r_max).iterator();
    }
}
