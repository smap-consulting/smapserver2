package org.smap.sdal.Utilities;

/*
 * The email was not sent and nothing about the message is at fault, so it should go back on
 * the queue and be tried again rather than being recorded as a failure.
 *
 * Covers anything that will pass on its own: the relay refusing because we have sent too
 * much of late, or no connection to it being free in time.  Distinct from an ordinary send
 * failure, where retrying the same message would fail the same way.
 */
public class EmailDeferredException extends Exception {

	private static final long serialVersionUID = 1L;

	public EmailDeferredException(String message) {
		super(message);
	}
}
