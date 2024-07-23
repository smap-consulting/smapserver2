-- Archive view records older than 100 days
WITH archived_rows AS (
    DELETE FROM log
    WHERE
        (event = 'view' or event = 'dashboard view' or event = 'API view' or event = 'API CSV view' or event = 'API single record view'
        	or event = 'user location view' or event = 'user acivity view'
        	or event = 'user'
        	or event = 'submissions'
        	or event = 'create survey'
        	or event = 'notification'
        	or event = 'error'
        	or event = 'block'
        	or event = 'task'
        	or event = 'email task'
        	or event = 'create pdf'
        	or event = 'import data'
        	or event = 'delete'
        	or event = 'restore'
        	or event = 'mailout'
        	or event = 'email')
        and log_time < now() - interval '100 days'
    RETURNING *
)
INSERT INTO log_archive
SELECT * FROM archived_rows;


-- Archive all log records older than 1 year
WITH archived_rows AS (
    DELETE FROM log
    WHERE   
        log_time < now() - interval '365 days'
    RETURNING *
)
INSERT INTO log_archive
SELECT * FROM archived_rows;

-- Truncate user trail for all records older than 100 days
DELETE FROM user_trail WHERE event_time < now() - interval '100 days';

-- Remove old uploads that reported an error
--delete from upload_event where not results_db_applied and upload_time < now() - interval '60 days';