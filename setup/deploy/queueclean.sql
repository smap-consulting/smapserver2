-- Clean submission queue of records that may have been partly processed when the subscriber stopped
update upload_event set queued = false where queued and not results_db_applied and ue_id not in (select ue_id from submission_queue );

-- Clean message queue of records that may have been partly processed when the subscriber stopped
update message set queued = false where queued and processed_time is null and id not in (select m_id from message_queue );