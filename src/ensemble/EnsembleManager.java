package ensemble;

import java.net.InetSocketAddress;
import java.util.List;

public interface EnsembleManager {
	public boolean newEnsemble(List<InetSocketAddress> bufferServerAddress) ;
	public void setEnsembleZnodePath(String path);
}
