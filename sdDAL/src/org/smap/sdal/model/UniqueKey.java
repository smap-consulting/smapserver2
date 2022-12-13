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

public class UniqueKey {
	
	public String key;
	public String key_policy;	
	public String group_survey_ident;
	
	public UniqueKey() {
	}
	
	public UniqueKey(String key, String key_policy, String group_survey_ident) {
		this.key = key;
		this.key_policy = key_policy;
		this.group_survey_ident = group_survey_ident;
	}
	
}
