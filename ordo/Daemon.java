package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import formats.Format;
import map.Mapper;
import map.Reducer;

public interface Daemon extends Remote {
	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;
	public void runReduce (Reducer m, List<Format> readers, Format writer, CallBack cb) throws RemoteException;
	public void runShuffle(int port, List<Format> readers,int nbReduce, SortComparator comp) throws RemoteException;
}
