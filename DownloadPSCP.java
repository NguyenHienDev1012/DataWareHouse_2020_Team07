package process;

import java.sql.SQLException;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;
import notification.SendMail;
public class DownloadPSCP {
	private String emailAddress="nguyenthanhhien.itnlu@gmail.com";
	static {
		try {
			System.loadLibrary("chilkat");  
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}
	public void downloadFilePSCP(){
		int id_scp=1;
		PSCPProcess pscp=new PSCPProcess();
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("");
		String hostname;
		try {
			hostname = pscp.selectField(id_scp, pscp.controlDB.getTable_name(), "host_name");
			int port = pscp.selectPortField(id_scp, pscp.controlDB.getTable_name(), "port_connect"); 
			boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
		}else{
				SendMail.sendMail(emailAddress, "CONNECT NOTIFICATION",
						"Connect fail!!");
				System.out.println("Connect fail!!");
			
			return;
		}
		ssh.put_IdleTimeoutMs(5000);
		String username=pscp.selectField(id_scp, pscp.controlDB.getTable_name(), "username");
		String pass=pscp.selectField(id_scp, pscp.controlDB.getTable_name(), "pass");
		success = ssh.AuthenticatePw(username, pass);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
		
			return;
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
		}else{
			SendMail.sendMail(emailAddress, "LOGIN NOTIFICATION",
					"Login fail!!");
			System.out.println("Login fail!!");
		
			return;
		}
		String file_name_architecture=pscp.selectField(id_scp, pscp.controlDB.getTable_name(), "file_name_architecture");
		 scp.put_SyncMustMatch(file_name_architecture);
		String remotePath = pscp.selectField(id_scp, pscp.controlDB.getTable_name(), "remotePath");
		String localPath = pscp.selectField(id_scp, pscp.controlDB.getTable_name(), "localPath");
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
		}else{
			SendMail.sendMail(emailAddress, "DOWNLOAD NOTIFICATION",
					"Download fail!!");
			System.out.println("Download fail!!");
			return;
		}
		SendMail.sendMail(emailAddress, "DOWNLOAD NOTIFICATION",
				"Download successfully!");
		System.out.println("Download successfully!");
    
		ssh.Disconnect();
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	////pscp -r -P 2227 guest_access@drive.ecepvn.org:/volume1/ECEP/song.nguyen/DW_2020/data/ D:\\DataWareHouse\\AllFile
}

