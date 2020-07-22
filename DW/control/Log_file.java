package control;

import java.util.Date;

public class Log_file {
    private int data_file_id;
    private String file_name;
    private int data_file_config_id;
    private String file_status;
    private int staging_load_count;
    private Date file_timestamp;
    private Date load_staging_timestamp;
    private Date load_warehouse_timestamp;
	public Log_file(int data_file_id, String file_name, int data_file_config_id, String file_status,
			int staging_load_count, Date file_timestamp, Date load_staging_timestamp, Date load_warehouse_timestamp) {
		this.data_file_id = data_file_id;
		this.file_name = file_name;
		this.data_file_config_id = data_file_config_id;
		this.file_status = file_status;
		this.staging_load_count = staging_load_count;
		this.file_timestamp = file_timestamp;
		this.load_staging_timestamp = load_staging_timestamp;
		this.load_warehouse_timestamp = load_warehouse_timestamp;
	}
	public int getData_file_id() {
		return data_file_id;
	}
	public void setData_file_id(int data_file_id) {
		this.data_file_id = data_file_id;
	}
	public String getFile_name() {
		return file_name;
	}
	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}
	public int getData_file_config_id() {
		return data_file_config_id;
	}
	public void setData_file_config_id(int data_file_config_id) {
		this.data_file_config_id = data_file_config_id;
	}
	public String getFile_status() {
		return file_status;
	}
	public void setFile_status(String file_status) {
		this.file_status = file_status;
	}
	public int getStaging_load_count() {
		return staging_load_count;
	}
	public void setStaging_load_count(int staging_load_count) {
		this.staging_load_count = staging_load_count;
	}
	public Date getFile_timestamp() {
		return file_timestamp;
	}
	public void setFile_timestamp(Date file_timestamp) {
		this.file_timestamp = file_timestamp;
	}
	public Date getLoad_staging_timestamp() {
		return load_staging_timestamp;
	}
	public void setLoad_staging_timestamp(Date load_staging_timestamp) {
		this.load_staging_timestamp = load_staging_timestamp;
	}
	public Date getLoad_warehouse_timestamp() {
		return load_warehouse_timestamp;
	}
	public void setLoad_warehouse_timestamp(Date load_warehouse_timestamp) {
		this.load_warehouse_timestamp = load_warehouse_timestamp;
	}
	@Override
	public String toString() {
		return "Log_file [data_file_id=" + data_file_id + ", file_name=" + file_name + ", data_file_config_id="
				+ data_file_config_id + ", file_status=" + file_status + ", staging_load_count=" + staging_load_count
				+ ", file_timestamp=" + file_timestamp + ", load_staging_timestamp=" + load_staging_timestamp
				+ ", load_warehouse_timestamp=" + load_warehouse_timestamp + "]";
	}
	
	
    
    
	
}
