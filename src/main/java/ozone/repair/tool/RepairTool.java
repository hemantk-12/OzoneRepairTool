package ozone.repair.tool;

import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RepairTool {

  public static void main(String[] args) throws RocksDBException {
    System.out.println(Arrays.toString(args));
    String omDbPath = args[0];
    String snapshotInfoTableKey = args[1];
    UUID newGlobalPreviousSnapshotId = UUID.fromString(args[2]);
    UUID newPathPreviousSnapshotId = UUID.fromString(args[3]);
    boolean dryRun = Boolean.parseBoolean(args[4]);

    List<ColumnFamilyDescriptor> cfDescriptors = getDbColumnFamilyDescriptor(omDbPath);
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    try (final DBOptions options = new DBOptions();
         final RocksDB db = RocksDB.open(options, omDbPath, cfDescriptors, cfHandles)) {
      ColumnFamilyHandle snapshotInfoCfh = getSnapshotInfoCfh(cfHandles);
      System.out.println("Table name: " + new String(snapshotInfoCfh.getName()));


      SnapshotInfo snapshotInfo = getSnapshotInfo(db, snapshotInfoCfh, snapshotInfoTableKey);
      if (snapshotInfo == null) {
        System.err.println("Snapshot: " + snapshotInfoTableKey + " doesn't not exist.");
        return;
      }

      System.out.println("Current Snapshot Info Volume: " + snapshotInfo.getVolumeName() +
          ", Bucket: " + snapshotInfo.getBucketName() +
          ", SnapshotName : " + snapshotInfo.getName() +
          ", SnapshotId: " + snapshotInfo.getSnapshotId() +
          ", GlobalPreviousSnapshotId: " + snapshotInfo.getGlobalPreviousSnapshotId() +
          ", PathPreviousSnapshotId: " + snapshotInfo.getPathPreviousSnapshotId());

      snapshotInfo.setGlobalPreviousSnapshotId(newGlobalPreviousSnapshotId);
      snapshotInfo.setPathPreviousSnapshotId(newPathPreviousSnapshotId);

      if (dryRun) {
        System.out.println("Snapshot Info will be updated to Volume: " + snapshotInfo.getVolumeName() +
            ", Bucket: " + snapshotInfo.getBucketName() +
            ", SnapshotName : " + snapshotInfo.getName() +
            ", SnapshotId: " + snapshotInfo.getSnapshotId() +
            ", GlobalPreviousSnapshotId: " + snapshotInfo.getGlobalPreviousSnapshotId() +
            ", PathPreviousSnapshotId: " + snapshotInfo.getPathPreviousSnapshotId());
      } else {
        byte[] snapshotInfoBytes = SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo);
        System.out.println("Updating snapshotInfo ...!!!");
        db.put(snapshotInfoCfh, snapshotInfoTableKey.getBytes(UTF_8), snapshotInfoBytes);

        SnapshotInfo updatedSnapshotInfo = getSnapshotInfo(db, snapshotInfoCfh, snapshotInfoTableKey);
        System.out.println("Snapshot Info is updated to Volume: " + updatedSnapshotInfo.getVolumeName() +
            ", Bucket: " + updatedSnapshotInfo.getBucketName() +
            ", SnapshotName : " + updatedSnapshotInfo.getName() +
            ", SnapshotId: " + updatedSnapshotInfo.getSnapshotId() +
            ", GlobalPreviousSnapshotId: " + updatedSnapshotInfo.getGlobalPreviousSnapshotId() +
            ", PathPreviousSnapshotId: " + updatedSnapshotInfo.getPathPreviousSnapshotId());
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
}
