package download;

import java.sql.SQLException;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import control.SCP_DownLoad;
import mail.MailConfig;
import mail.SendMail;

public class DownloadPSCP {
	private PSCPProcess pscpProcess = new PSCPProcess();
	private String table_scp_download = "scp_download";

	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Native code library failed to load.\n"+e);
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public void downloadFilePSCP(int id_scp) throws SQLException {
		SCP_DownLoad scp_download = pscpProcess.selectAllField(id_scp, table_scp_download);
		System.out.println(scp_download.toString());
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("H");
		
		String hostname;
		hostname = scp_download.getHostname();
		int port = scp_download.getPortconnect();
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, ssh.lastErrorText());
			System.out.println(ssh.lastErrorText());
			return;
		}
		ssh.put_IdleTimeoutMs(5000);
		
		String username = scp_download.getUsername();
		String pass = scp_download.getPassword();
		success = ssh.AuthenticatePw(username, pass);
		if (success != true) {
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, ssh.lastErrorText());
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, ssh.lastErrorText());
			System.out.println(scp.lastErrorText());
			return;
		}
		String file_name_architecture = scp_download.getFile_name_architecture();
		scp.put_SyncMustMatch(file_name_architecture);
		String remotePath = scp_download.getRemotePath();
		String localPath = scp_download.getLocalPath();
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		
		if (success != true) {
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, ssh.lastErrorText());
			System.out.println(scp.lastErrorText());
			return;
		}
		SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Download successfully!");
		System.out.println("Download successfully!");

		ssh.Disconnect();
	}
// Phương thức này để cập nhật trạng thái trong log_file khi file đã download về.
	public void loadFileStatus(DataProcess dp, String table_name, int id_config) throws SQLException{
	       // Lấy thời gian hiện tại.
	       	String timestamp= getCurrentTime();
	       // Lấy tất cả dữ liệu các trường configuration theo id_config 
			Configuration configuration=dp.getControlDb().selectAllFieldConfigurationByConfigId((id_config));
			String import_dir = configuration.getImport_dir();
			int config_id = configuration.getConfig_id();
			//Lấy tất cả tên của các file đã ghi vào log_file
			ArrayList<String> listFileNameCurrent=dp.getControlDb().selectAllFileNameInLogFile("log_file");

			File imp_dir = new File(import_dir);
			if (imp_dir.exists()) {
				File[] listFile = imp_dir.listFiles();
				if(listFile.length>0){
				for(File f: listFile){
					if(listFileNameCurrent.size()>0){
						// Nếu danh sách tên file hiện tại trong log mà không chứa tên file ta muốn ghi log thì tiến hành ghi log.
						if(!listFileNameCurrent.contains(f.getName())){
							dp.getControlDb().insertLogFileStatus(table_name, f.getName(), config_id, FILE_STATUS_READY,timestamp);
					}
				}
					else{
						dp.getControlDb().insertLogFileStatus(table_name, f.getName(), config_id, FILE_STATUS_READY,timestamp);
					}
				}
				
				SendMail.sendMail(MailConfig.EMAIL_RECEIVER, "URGENT FILE INFORMATION",
						"Load file status successfully!");
				System.out.println("Load file status successfully!");
			}
				else{
					SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE,
							"No any file here to load file status!");
					System.out.println("No any file here to load file status!");
				}
				
			}
			else{
				System.out.println("Path not exists!!!");
				return;
			}
			
		}
	//// pscp -r -P 2227
	//// guest_access@drive.ecepvn.org:/volume1/ECEP/song.nguyen/DW_2020/data/
	//// D:\\DataWareHouse\\AllFile
}
