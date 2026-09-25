package model;

import java.util.ArrayList;

/*
 * Webhook payload sent by the Meta WhatsApp Cloud API
 * Only the parts needed to receive a text message are included
 */
public class MessageWhatsApp {
	public String object;
	public ArrayList<Entry> entry;

	public static class Entry {
		public String id;
		public ArrayList<Change> changes;
	}

	public static class Change {
		public String field;
		public Value value;
	}

	public static class Value {
		public String messaging_product;
		public Metadata metadata;
		public ArrayList<Message> messages;
	}

	public static class Metadata {
		public String display_phone_number;
		public String phone_number_id;
	}

	public static class Message {
		public String from;
		public String id;
		public String timestamp;	// Unix time in seconds
		public String type;
		public Text text;
	}

	public static class Text {
		public String body;
	}
}
