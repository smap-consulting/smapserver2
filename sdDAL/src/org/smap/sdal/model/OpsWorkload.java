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

package org.smap.sdal.model;

/*
 * One person's open work in the Operations overview
 */
public class OpsWorkload {
	public String ident;
	public String name;
	public int openCases;		// open cases the person is an owner of
	public int openTasks;
	public int overdue;			// overdue tasks
	public double overduePct;	// overdue / openTasks * 100
	public long oldestDays;		// age of the person's oldest open case or task
	public String rag;			// green || amber || red

	public OpsWorkload(String ident) {
		this.ident = ident;
	}
}
