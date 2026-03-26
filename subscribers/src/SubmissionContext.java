
/*****************************************************************************
This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

/**
 * Thread-local holder for the current submission's instanceId.
 * Set by SubmissionProcessor and SubEventProcessor when processing begins,
 * cleared in the finally block. Used by SmapLogFormatter to include a
 * trace identifier on every log line without passing it through every call.
 */
public class SubmissionContext {

	private static final ThreadLocal<String> instanceId = new ThreadLocal<>();

	public static void set(String id) {
		instanceId.set(id);
	}

	/** Returns the current instanceId, or "-" if none is set. */
	public static String get() {
		String id = instanceId.get();
		return (id != null) ? id : "-";
	}

	public static void clear() {
		instanceId.remove();
	}
}
