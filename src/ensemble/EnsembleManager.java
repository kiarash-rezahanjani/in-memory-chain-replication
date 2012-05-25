package ensemble;

import java.net.InetSocketAddress;
import java.util.List;

import coordination.Protocol.EnsembleBean;

public interface EnsembleManager {
	public boolean newEnsemble(EnsembleBean ensembleBean) ;
	public void cleanEnsemble();
	public void setEnsembleZnodePath(String path, boolean isLeader);
}
