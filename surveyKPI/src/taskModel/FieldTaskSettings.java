package taskModel;

public class FieldTaskSettings {
	public boolean ft_delete_submitted;	// deprecated
	public String ft_delete;
	public String ft_send_location;
	public boolean ft_location_trigger;
	public boolean ft_sync_incomplete;
	public boolean ft_odk_style_menus;
	public boolean ft_specify_instancename;
	public boolean ft_mark_finalized;
	public boolean ft_prevent_disable_track;
	public String ft_enable_geofence;
	public boolean ft_admin_menu;
	public boolean ft_server_menu;
	public boolean ft_meta_menu;
	public boolean ft_exit_track_menu;
	public boolean ft_bg_stop_menu;
	public boolean ft_review_final;
	public boolean ft_send_wifi;			// deprecated
	public boolean ft_send_wifi_cell;	// deprecated
	public String ft_send;
	public String ft_image_size;
	public int ft_pw_policy;
	public String ft_backward_navigation;
	public String ft_high_res_video;
	public String ft_navigation;
	public String ft_guidance;
	public String ft_input_method;
	public int ft_im_ri;
	public int ft_im_acc;
	
	public void setFtEnableGeofence(boolean v) {
		if(v) {
			ft_enable_geofence = "on";
		} else {
			ft_enable_geofence = "off";
		}
	}
}
