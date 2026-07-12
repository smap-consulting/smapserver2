package org.smap.sdal.model;

/*
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

*/

/*
 * A static pseudo-SQL filter restricting the reference data a survey bundle (the linker)
 * pulls from a source survey.  Defined at the group level, per (linker, source) pair.
 */
public class ReferenceFilter {
	public int id;
	public String linkerSIdent;		// Requesting survey group ident
	public String linkedSIdent;		// Source survey group ident
	public String filter;			// Pseudo-SQL filter e.g. ${status} = 'open'
	public boolean enabled = true;
	public int maxRecords = 0;		// Cap on records supplied over this connection.  0 = unlimited
	public String linkedSName;		// Source survey display name (for the UI, not persisted)

	public ReferenceFilter() {
	}

	public ReferenceFilter(String linkerSIdent, String linkedSIdent, String filter) {
		this.linkerSIdent = linkerSIdent;
		this.linkedSIdent = linkedSIdent;
		this.filter = filter;
	}
}
