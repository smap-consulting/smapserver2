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
 * Purpose: Allow the user to select a web form in order to complete a survey
 */
var gUserLocale = navigator.language;
if (Modernizr.localstorage) {
	gUserLocale = localStorage.getItem('user_locale') || navigator.language;
}

var gDelSig = false;

requirejs.config({
	baseUrl: 'js/libs',
	waitSeconds: 0,
	locale: gUserLocale,
	paths: {
		app: '../app',
		i18n: '../../../../js/libs/i18n',
		async: '../../../../js/libs/async',
		localise: '../../../../js/app/localise',
		modernizr: '../../../../js/libs/modernizr',
		common: '../../../../js/app/common',
		globals: '../../../../js/app/globals',
		lang_location: '../../../../js'
	},
	shim: {
		'common': ['jquery']
	}
});

require([
	'jquery',
	'common',
	'globals',
	'localise'
], function($, common, globals, localise) {

	$(document).ready(function() {

		setCustomWebForms();			// Apply custom javascript
		setupUserProfile(true);
		localise.setlang();		// Localise HTML

		// Get the user details
		globals.gIsAdministrator = false;
		getLoggedInUser(projectSet, false, true, undefined);

		// Set change function on projects
		$('#project_name').change(function() {
			globals.gCurrentProject = $('#project_name option:selected').val();
			globals.gCurrentSurvey = -1;
			globals.gCurrentTaskGroup = undefined;

			getSurveysForList(globals.gCurrentProject);			// Get surveys

			saveCurrentProject(globals.gCurrentProject,
				globals.gCurrentSurvey,
				globals.gCurrentTaskGroup);
		});

		// Refresh menu
		$('#m_refresh').click(function () {
			$('.up_alert').hide();
			projectSet();
		});

		/*
		 * Alerts
		 */
		$('#show_alerts').click(function(){
			if(!globals.gAlertSeen) {
				globals.gAlertSeen = true;
				$('.alert_icon').removeClass("text-danger");
				saveLastAlert(globals.gLastAlertTime, true);
			}
		});

		/*
		 * Register service worker
		 */
		if ('serviceWorker' in navigator) {
				navigator.serviceWorker.register('/myWorkServiceWorker.js').then(function(registration) {
					// Registration was successful
					console.log('ServiceWorker registration successful with scope: ', registration.scope);
				}, function(err) {
					// registration failed :(
					console.log('ServiceWorker registration failed: ', err);
				});
		}

	});


	function projectSet() {
		getSurveysForList(globals.gCurrentProject);			// Get surveys
		//getAlerts();
	}

	function getSurveysForList(projectId) {

		url="/surveyKPI/myassignments";

		addHourglass();
		$.ajax({
			url: url,
			dataType: 'json',
			cache: false,
			success: function(data) {
				var filterProject = projectId;
				removeHourglass();
				completeSurveyList(data, filterProject);
			},
			error: function(xhr, textStatus, err) {
				removeHourglass();
				if(xhr.readyState == 0 || xhr.status == 0) {
					return;  // Not an error
				} else {
					console.log("Error: Failed to get list of surveys: " + err);
				}
			}
		});
	}

	/*
	 * Fill in the survey list
	 */
	function completeSurveyList(surveyList, filterProjectId) {

		var i,
			h = [],
			idx = -1,
			formList = surveyList.forms,
			taskList = surveyList.data;



		// Add the tasks
		if (taskList) {
			addTaskList(taskList, filterProjectId);
		} else {
			$('#tasks_count').html('(0)');
			$('#task_list').html('');
		}

		// Add the forms
		if (formList) {
			addFormList(formList, filterProjectId);
		} else {
			$('#forms_count').html('(0)');
			$('#form_list').html('');
		}
	}

	function addFormList(formList, filterProjectId) {
		var i,
			h = [],
			idx = -1,
			$formList = $('#form_list'),
			count = 0;

		for(i = 0; i < formList.length; i++) {
			if(!filterProjectId || filterProjectId == formList[i].pid) {
				h[++idx] = '<a role="button" class="btn btn-primary btn-block btn-lg" target="_blank" href="/webForm/';
				h[++idx] = formList[i].ident;
				h[++idx] = '">';
				h[++idx] = formList[i].name;
				h[++idx] = '</a>';
				count++;
			}
		}
		$('#forms_count').html('(' + count+ ')');
		$formList.html(h.join(''));
	}

	function addTaskList(taskList, filterProjectId) {
		var i,
			h = [],
			idx = -1,
			$taskList = $('#task_list'),
			count = 0;

		for(i = 0; i < taskList.length; i++) {

			if(!filterProjectId || filterProjectId == taskList[i].task.pid) {
				repeat = taskList[i].task.repeat;	// Can complete the task multiple times
				h[++idx] = '<div class="btn-group btn-block btn-group-lg d-flex" role="group" aria-label="Button group for task selection or rejection">';
				h[++idx] = '<a id="a_';
				h[++idx] = taskList[i].assignment.assignment_id;
				h[++idx] = '" class="task btn btn-warning w-80" role="button" target="_blank" data-repeat="';

				if(repeat) {
					h[++idx] = 'true';
				} else {
					h[++idx] = 'false';
				}
				h[++idx] = '" href="/webForm/';
				h[++idx] = taskList[i].task.form_id;

				var hasParam = false;
				if(taskList[i].task.initial_data_source) {
					if (taskList[i].task.initial_data_source === 'survey' && taskList[i].task.update_id) {

						h[++idx] = (hasParam ? '&' : '?');
						h[++idx] = 'datakey=instanceid&datakeyvalue=';
						h[++idx] = taskList[i].task.update_id;
						hasParam = true;

					} else if (taskList[i].task.initial_data_source === 'task') {
						h[++idx] = (hasParam ? '&' : '?');
						h[++idx] = 'taskkey=';
						h[++idx] = taskList[i].task.id;
						hasParam = true;
					}
				}
				// Add the assignment id
				h[++idx] = (hasParam ? '&' : '?');
				h[++idx] = 'assignment_id=';
				h[++idx] = taskList[i].assignment.assignment_id;

				h[++idx] = '">';
				h[++idx] = taskList[i].task.title + " (" + localise.set["c_id"] + ": " + taskList[i].assignment.assignment_id + ")";
				h[++idx] = '</a>';

				// Add button with additional options
				h[++idx] = '<button ';
				h[++idx] = 	'id="a_r_' + taskList[i].assignment.assignment_id;
				h[++idx] = '" class="btn btn-info w-20 reject" type="button"';
				h[++idx] = '" data-aid="';
				h[++idx] = taskList[i].assignment.assignment_id;
				h[++idx] = '">';
				h[++idx] = localise.set["c_reject"]
				h[++idx] = '</button>';

				h[++idx] = '</div>';        // input group
				count++;
			}
		}

		$('#tasks_count').html('(' + count + ')');
		$taskList.html(h.join(''));

		$taskList.find('.task').off().click(function(){
			$('.up_alert').hide();
			var $this = $(this),
				repeat = $this.data("repeat");

			if(!repeat) {
				$this.removeClass('btn-warning').addClass('btn-success');		// Mark task as done
				$this.addClass('disabled');
				$this.closest(".btn-group").find(".reject").addClass("disabled");
			}
		});

		$taskList.find('.reject').off().click(function(){
			var $this = $(this);

			if(!$this.hasClass('disabled')) {
				reject($this.data("aid"));
			}
		});
	}


	function reject(aid) {

		$('.up_alert').hide();
		bootbox.prompt({
			title: localise.set["a_res_5"],
			centerVertical: true,
			locale: gUserLocale,
			callback: function(result){
				console.log(result);

				// Validate
				if(!result || result.trim().length < 5) {
					$('.up_alert').show().removeClass('alert-success').addClass('alert-danger').html(localise.set["a_res_5"]);
					return;
				}

				var assignment = {
					assignment_id: aid,
					assignment_status: 'rejected',
					task_comment: result
				}
				var assignmentString = JSON.stringify(assignment);

				addHourglass();
				$.ajax({
					type: "POST",
					data: {assignment: assignmentString},
					cache: false,
					contentType: "application/json",
					url: "/surveyKPI/myassignments/update_status",
					success: function(data, status) {
						removeHourglass();
						$('#a_' + aid).removeClass('btn-warning').addClass('btn-danger');
						$('#a_r_' + aid).addClass('disabled');


					},
					error: function(xhr, textStatus, err) {
						removeHourglass();
						$('.up_alert').show().removeClass('alert-success').addClass('alert-danger').html(localise.set["msg_err_upd"] + xhr.responseText);

					}
				});
			}
		});


	}

});

