package org.smap.sdal.Utilities;

/*
 * The relay is refusing mail because we have sent too much of it, not because there is
 * anything wrong with this message.  It is worth telling apart from an ordinary send
 * failure: the message should go back on the queue and be sent when the relay is willing
 * again, rather than being marked processed and lost.
 */
public class EmailRateLimitException extends Exception {

	private static final long serialVersionUID = 1L;

	public EmailRateLimitException(String message) {
		super(message);
	}
}
