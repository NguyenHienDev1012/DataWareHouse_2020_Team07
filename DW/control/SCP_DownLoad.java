package control;

public class SCP_DownLoad {
	
	private int id;
	private String hostname;
	private int portconnect;
	private String username;
	private String password;
	private String file_name_architecture;
	private String remotePath;
	private String localPath;
	private int config_id;
	
	public SCP_DownLoad(int id, String hostname, int portconnect, String username, String password,
			String file_name_architecture, String remotePath, String localPath, int config_id) {
		this.id = id;
		this.hostname = hostname;
		this.portconnect = portconnect;
		this.username = username;
		this.password = password;
		this.file_name_architecture = file_name_architecture;
		this.remotePath = remotePath;
		this.localPath = localPath;
		this.config_id = config_id;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getPortconnect() {
		return portconnect;
	}

	public void setPortconnect(int portconnect) {
		this.portconnect = portconnect;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getFile_name_architecture() {
		return file_name_architecture;
	}

	public void setFile_name_architecture(String file_name_architecture) {
		this.file_name_architecture = file_name_architecture;
	}

	public String getRemotePath() {
		return remotePath;
	}

	public void setRemotePath(String remotePath) {
		this.remotePath = remotePath;
	}

	public String getLocalPath() {
		return localPath;
	}

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}

	public int getConfig_id() {
		return config_id;
	}

	public void setConfig_id(int config_id) {
		this.config_id = config_id;
	}

	@Override
	public String toString() {
		return "SCP_DownLoad [id=" + id + ", hostname=" + hostname + ", portconnect=" + portconnect + ", username="
				+ username + ", password=" + password + ", file_name_architecture=" + file_name_architecture
				+ ", remotePath=" + remotePath + ", localPath=" + localPath + ", config_id=" + config_id + "]";
	}
	
	
	
}
