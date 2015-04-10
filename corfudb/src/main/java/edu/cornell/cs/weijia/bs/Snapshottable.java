/**
 * 
 */
package edu.cornell.cs.weijia.bs;

import java.util.List;


/**
 * @author sonic
 *
 */
public interface Snapshottable {
  
  public void createSnapshot(Object sid) throws BlockSnapshotException;
  public void deleteSnapshot(Object sid) throws BlockSnapshotException;
  public void deleteSnapshot(Object sid,boolean bGC) throws BlockSnapshotException;
  public List<Object> getSnapshotIDList() throws BlockSnapshotException;
  public void read(int offset, int length, byte[]buf, Object sid)throws BlockSnapshotException;
  public int getLen(Object sid)throws BlockSnapshotException;
}
