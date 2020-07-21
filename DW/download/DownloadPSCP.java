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
		SCP_DownLoad scp_downlowd = pscpProcess.selectAllField(id_scp, table_scp_download);
		System.out.println(scp_downlowd.toString());
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("H");
		String hostname;
		hostname = scp_downlowd.getHostname();
		int port = scp_downlowd.getPortconnect();
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, ssh.lastErrorText());
			System.out.println(ssh.lastErrorText());
			return;
		}
		ssh.put_IdleTimeoutMs(5000);
		String username = scp_downlowd.getUsername();
		String pass = scp_downlowd.getPassword();
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
		String file_name_architecture = scp_downlowd.getFile_name_architecture();
		scp.put_SyncMustMatch(file_name_architecture);
		String remotePath = scp_downlowd.getRemotePath();
		String localPath = scp_downlowd.getLocalPath();
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

	//// pscp -r -P 2227
	//// guest_access@drive.ecepvn.org:/volume1/ECEP/song.nguyen/DW_2020/data/
	//// D:\\DataWareHouse\\AllFile
}
