insert into 
    stg_oie_housing_apps
select 
    to_number(entryapplicationid) application_id,
    substr(hsuid, 1, 9) emplid,
    entryname applicant_name,
    termdescription residence_session,
    classificationdescription1 student_category,
    applicationstatusdescription2 application_status,
    to_date(replace(receiveddate,'nan',''), 'MM/DD/YYYY HH24:MI') app_rcvd_dttm,
    trunc(to_date(replace(finalcomplete,'nan',''), 'MM/DD/YYYY HH24:MI')) app_completed_date,
    trunc(to_date(replace(offersentdate,'nan',''), 'MM/DD/YYYY HH24:MI')) offer_sent_date,
    trunc(to_date(replace(canceldate,'nan',''), 'MM/DD/YYYY HH24:MI')) app_cancel_date,
    case 
        when to_date(replace(receiveddate,'nan',''), 'MM/DD/YYYY HH24:MI') is null
            then 0 
        else 1 
    end application_count,
    case 
        when trunc(to_date(replace(finalcomplete,'nan',''), 'MM/DD/YYYY HH24:MI')) is null 
            then 0 
        else 1 
    end complete_application_count,
    case 
        when trunc(to_date(replace(offersentdate,'nan',''), 'MM/DD/YYYY HH24:MI')) is null 
            then 0 
        else 1 
    end offer_sent_count,
    case 
        when trunc(to_date(replace(canceldate,'nan',''), 'MM/DD/YYYY HH24:MI')) is null
            then 0 
        else 1 
    end cancelled_application_count,
    'HSNG' data_origin,
    systimestamp created_ew_dttm,
    'N' loaded
from extr_oie_housing_apps
