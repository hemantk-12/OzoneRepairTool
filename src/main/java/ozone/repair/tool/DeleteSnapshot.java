package ozone.repair.tool;

import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED;

public class DeleteSnapshot {
  public static void main(String[] args) throws RocksDBException {
    System.out.println(Arrays.toString(args));
    String omDbPath = args[0];
    String filePath = args[1];
    boolean dryRun = Boolean.parseBoolean(args[2]);
    List<String> snapshotTableKeys = readSnapshotKeysFromFile(filePath);
    System.out.println("Snapshot to be marked deleted: " + snapshotTableKeys);

    List<ColumnFamilyDescriptor> cfDescriptors = getDbColumnFamilyDescriptor(omDbPath);
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    try (final DBOptions options = new DBOptions();
         final RocksDB db = RocksDB.open(options, omDbPath, cfDescriptors, cfHandles);
         ColumnFamilyHandle snapshotInfoCfh = getSnapshotInfoCfh(cfHandles)) {

      for (String snapshotTableKey: snapshotTableKeys) {
        SnapshotInfo snapshotInfo = getSnapshotInfo(db, snapshotInfoCfh, snapshotTableKey);
        assert snapshotInfo != null;

        System.out.println("Marking snapshot: " + snapshotInfo.getName() +
            ", Volume: " + snapshotInfo.getVolumeName() +
            ", Bucket: " + snapshotInfo.getBucketName() +
            ", SnapshotId: " + snapshotInfo.getSnapshotId() +
            ", Current status: " + snapshotInfo.getSnapshotStatus() +
            " deleted.");

        snapshotInfo.setSnapshotStatus(SNAPSHOT_DELETED);
        snapshotInfo.setDeletionTime(System.currentTimeMillis());

        if (!dryRun) {
          System.out.println("Marking snapshot delete.");
          db.put(snapshotInfoCfh, snapshotTableKey.getBytes(UTF_8),
              SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo));
          SnapshotInfo updatedSnapshotInfo = getSnapshotInfo(db, snapshotInfoCfh, snapshotTableKey);
          if (updatedSnapshotInfo == null || updatedSnapshotInfo.getSnapshotStatus() != SNAPSHOT_DELETED) {
            System.out.println("Something happened while adding snapshotInfo to the Table.");
          } else {
            System.out.println("Marked snapshot: " + snapshotInfo.getName() +
                " deleted for volume: " + snapshotInfo.getVolumeName() +
                ", Bucket: " + snapshotInfo.getBucketName() +
                ", SnapshotId: " + snapshotInfo.getSnapshotId());
          }
        }
      }

    } catch (IOException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<ColumnFamilyDescriptor> getDbColumnFamilyDescriptor(String omDbPath) throws RocksDBException {
    try (final Options options = new Options()) {
      List<byte[]> bytes = RocksDB.listColumnFamilies(options, omDbPath);
      return bytes.stream().map(ColumnFamilyDescriptor::new).collect(Collectors.toList());
    }
  }

  private static ColumnFamilyHandle getSnapshotInfoCfh(List<ColumnFamilyHandle> cfHandleList) throws RocksDBException {
    String SNAPSHOT_INFO_TABLE = "snapshotInfoTable";
    byte[] nameBytes = SNAPSHOT_INFO_TABLE.getBytes(UTF_8);

    for (ColumnFamilyHandle cf : cfHandleList) {
      if (Arrays.equals(cf.getName(), nameBytes)) {
        return cf;
      }
    }

    return null;
  }

  private static SnapshotInfo getSnapshotInfo(RocksDB db, ColumnFamilyHandle snapshotInfoCfh, String snapshotInfoLKey)
      throws IOException, RocksDBException {
    byte[] bytes = db.get(snapshotInfoCfh, snapshotInfoLKey.getBytes(UTF_8));
    return bytes != null ? SnapshotInfo.getCodec().fromPersistedFormat(bytes) : null;
  }

  private static List<String> readSnapshotKeysFromFile(String filePath) {
    List<String> snapshotTableKeys = new ArrayList<>();
    File file = new File(filePath);
    try (Stream<String> stream = Files.lines(file.toPath())) {
      stream.forEach(snapshotTableKeys::add);
    } catch (IOException exception) {
      throw new RuntimeException("Failed to read files." + exception);
    }
    return snapshotTableKeys;
  }
}
