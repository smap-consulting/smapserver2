package org.smap.sdal.Utilities;

/*
 * Secrets in settings, such as passwords, API tokens and private keys, are never sent to the
 * browser.  A secret that is set is sent as PLACEHOLDER instead.  The settings page shows it in its
 * field and sends it back unchanged, and a PLACEHOLDER coming back means keep the stored value.
 * Anything else replaces it, so emptying the field still clears the secret.
 */
public class Secrets {

	public static final String PLACEHOLDER = "********";

	private Secrets() {
	}

	/*
	 * What to send to the browser for a stored secret
	 */
	public static String hide(String stored) {
		return (stored == null || stored.isEmpty()) ? stored : PLACEHOLDER;
	}

	/*
	 * What to store for a secret sent from the browser
	 */
	public static String keep(String incoming, String stored) {
		return PLACEHOLDER.equals(incoming) ? stored : incoming;
	}
}
