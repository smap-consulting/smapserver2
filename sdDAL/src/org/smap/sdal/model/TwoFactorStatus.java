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
 * The state of a user's two factor authentication, and - during enrolment only - the
 * secret they need to add to their authenticator app.
 *
 * secret, otpauthUrl and qrPng are set only by the enrol call and are never returned once
 * enrolment is confirmed.
 */
public class TwoFactorStatus {

	public boolean enabled;			// Enrolment is complete and codes are required
	public boolean pending;			// A secret exists but no code has confirmed it yet
	public String enrolled;			// When enrolment was confirmed

	public String secret;			// base32, for manual entry
	public String otpauthUrl;
	public String qrPng;			// data: URI
}
