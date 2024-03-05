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

public class RocksDBRepair {

  public static void main(String[] args) throws RocksDBException {
    System.out.println(Arrays.toString(args));
    String omDbPath = args[0];
    boolean dryRun = Boolean.parseBoolean(args[1]);

    List<ColumnFamilyDescriptor> cfDescriptors = getDbColumnFamilyDescriptor(omDbPath);
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    try (final DBOptions options = new DBOptions();
         final RocksDB db = RocksDB.open(options, omDbPath, cfDescriptors, cfHandles);
         ColumnFamilyHandle snapshotInfoCfh = getSnapshotInfoCfh(cfHandles)) {

      System.out.println("Table name: " + new String(snapshotInfoCfh.getName()));

      String snapshotInfoTableKey = "/hat/ozdata/hatozdatasnapshot0104";

      SnapshotInfo snapshotInfo = new SnapshotInfo.Builder()
          .setSnapshotId(UUID.fromString("a2f8751f-09d3-4617-8740-ba0dedff0fc2"))
          .setName("hatozdatasnapshot0104")
          .setVolumeName("hat")
          .setBucketName("ozdata")
          .setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED)
          .setCreationTime(1704284894386L)
          .setDeletionTime(1704285098540L)
          .setGlobalPreviousSnapshotId(UUID.fromString("bad50e13-c353-4ae2-b223-1f543a60b222"))
          .setPathPreviousSnapshotId(UUID.fromString("bad50e13-c353-4ae2-b223-1f543a60b222"))
          .setSnapshotPath("hat/ozdata")
          .setCheckpointDir("-a2f8751f-09d3-4617-8740-ba0dedff0fc2")
          .setDbTxSequenceNumber(256964L)
          .setDeepClean(false)
          .setSstFiltered(true)
          .build();


      if (dryRun) {
        System.out.println("Adding snapshotInfo will be added to the table, volume: " + snapshotInfo.getVolumeName() +
            ", Bucket: " + snapshotInfo.getBucketName() +
            ", SnapshotName : " + snapshotInfo.getName() +
            ", SnapshotId: " + snapshotInfo.getSnapshotId() +
            ", GlobalPreviousSnapshotId: " + snapshotInfo.getGlobalPreviousSnapshotId() +
            ", PathPreviousSnapshotId: " + snapshotInfo.getPathPreviousSnapshotId());
      } else {
        System.out.println("Updating snapshotInfo ...!!!");
        db.put(snapshotInfoCfh, snapshotInfoTableKey.getBytes(UTF_8),
            SnapshotInfo.getCodec().toPersistedFormat(snapshotInfo));

        SnapshotInfo updatedSnapshotInfo = getSnapshotInfo(db, snapshotInfoCfh, snapshotInfoTableKey);
        if (updatedSnapshotInfo != null) {
          System.out.println("Added snapshotInfo to the table, volume: " + updatedSnapshotInfo.getVolumeName() +
              ", Bucket: " + updatedSnapshotInfo.getBucketName() +
              ", SnapshotName : " + updatedSnapshotInfo.getName() +
              ", SnapshotId: " + updatedSnapshotInfo.getSnapshotId() +
              ", GlobalPreviousSnapshotId: " + updatedSnapshotInfo.getGlobalPreviousSnapshotId() +
              ", PathPreviousSnapshotId: " + updatedSnapshotInfo.getPathPreviousSnapshotId());
        } else {
          System.out.println("Something happened while adding snapshotInfo to the Table.");
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
}
