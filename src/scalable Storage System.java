import java.util.logging.Logger;

public class StorageManager {
    private DistributedStorage storage;
    private static final Logger logger = Logger.getLogger(StorageManager.class.getName());

    public StorageManager() {
        storage = new DistributedStorage("CREODIAS-ObjectStore");
    }

    public void save(Frame data) {
        String path = "/satellite/" + data.getTimestamp() + ".tif";
        try {
            storage.write(path, data.toBytes());
            logger.info("Successfully stored data at: " + path);
        } catch (StorageException e) {
            logger.severe("Failed to store data at: " + path + ". Error: " + e.getMessage());
        } catch (Exception e) {
            logger.severe("Unexpected error while storing data at: " + path + ". Error: " + e.getMessage());
        }
    }

    public Frame retrieve(String timestamp) {
        String path = "/satellite/" + timestamp + ".tif";
        try {
            byte[] dataBytes = storage.read(path);
            logger.info("Successfully retrieved data from: " + path);
            return Frame.fromBytes(dataBytes);
        } catch (StorageException e) {
            logger.severe("Failed to retrieve data from: " + path + ". Error: " + e.getMessage());
        } catch (Exception e) {
            logger.severe("Unexpected error while retrieving data from: " + path + ". Error: " + e.getMessage());
        }
        return null;
    }
}