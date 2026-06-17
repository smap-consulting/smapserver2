package org.smap.sdal.model;

/*
 * Records a single changed survey setting (old value -> new value)
 * Stored as part of a ChangeElement for "settings_update" changes
 */
public class SettingChange {

	public String label;	// Localised setting name
	public String oldVal;	// Value before the change
	public String newVal;	// Value after the change

	public SettingChange() {
	}

	public SettingChange(String label, String oldVal, String newVal) {
		this.label = label;
		this.oldVal = oldVal;
		this.newVal = newVal;
	}
}
