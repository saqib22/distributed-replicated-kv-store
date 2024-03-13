package de.tum.i13.server.echo;

import de.tum.i13.client.ActiveConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SyncReplicas extends Thread{
    private int wal_size;
    private WAL wal;
    private int seek;
    private final Logger logger;
    public SyncReplicas(WAL wal, Logger logger){
        this.wal_size = 0;
        this.seek = 0;
        this.wal = wal;
        this.logger = logger;
    }

    @Override
    public void run() {

        while(true){
            try {
                TimeUnit.MILLISECONDS.sleep(100);

                if (this.wal.getNewVals() > 0){
                    for (int i = 0; i<this.wal.getNewVals(); i++){
                        logger.info("Syncing the replicas of this nodes");
                        String result = this.wal.wal_process("get", String.valueOf(this.seek));

                        ActiveConnection connection1 = EchoLogic.get_target_replica1_connection();
                        connection1.write(result.replace("put", "put_replica") + "\r\n");
                        String response1 = connection1.readline();

                        ActiveConnection connection2 = EchoLogic.get_target_replica2_connection();
                        connection2.write(result.replace("put", "put_replica") + "\r\n");
                        String response2 = connection2.readline();

                        if ((response1.contains("put_success") || response1.contains("put_update")) &&
                        (response2.contains("put_success") || response2.contains("put_update"))){
                            String seek_val = this.wal.wal_process("update_sync", String.valueOf(this.seek));
                            this.seek = Integer.parseInt(seek_val);
                            this.wal.setNewVals(this.wal.getNewVals()-1);
                            logger.info("Synchronized " + result);
                        }
                        else {
                            this.wal.setNewVals(0);
                            break;
                        }
                    }
                }

            /*} catch (InterruptedException e) {
                throw new RuntimeException(e);*/
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


        }

    }
}
