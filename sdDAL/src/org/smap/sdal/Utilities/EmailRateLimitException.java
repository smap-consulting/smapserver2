package org.smap.sdal.Utilities;

/*
 * The relay is refusing mail because we have sent too much of it, not because there is
 * anything wrong with this message.  Deferred rather than failed: the message goes back on
 * the queue and is sent when the relay is willing again.
 */
public class EmailRateLimitException extends EmailDeferredException {

	private static final long serialVersionUID = 1L;

	public EmailRateLimitException(String message) {
		super(message);
	}
}
