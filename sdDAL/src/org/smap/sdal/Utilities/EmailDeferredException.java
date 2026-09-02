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

	/*
	 * What could not be sent, in the words the monitor uses, so that a message waiting on a
	 * relay can be shown there as itself rather than as a bare id.  Set by whoever has the
	 * detail on the way out, which is well below the code that decides what to do about it.
	 */
	private String notifyDetails;

	public EmailDeferredException(String message) {
		super(message);
	}

	public String getNotifyDetails() {
		return notifyDetails;
	}

	public void setNotifyDetails(String notifyDetails) {
		if(notifyDetails != null) {
			this.notifyDetails = notifyDetails;
		}
	}
}
