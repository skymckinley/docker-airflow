declare
   cursor c_app_recs is (
        select *
        from stg_oie_housing_apps
        where loaded <> 'Y'
   );
   
    r_dim_row oie_ws.dim_oie_housing_application%rowtype;
   
    l_hash_val pkg_oie_etl.etl_hash_t;
   
    l_run_date constant date := trunc(sysdate);

    l_housing_app_key dim_oie_housing_application.housing_app_key%type;
    l_mismatched_rows_found number;
   
begin
    for stg in c_app_recs loop
        select standard_hash(
            stg.emplid ||
            stg.applicant_name ||
            stg.residence_session ||
            stg.student_category ||
            stg.application_status)
            into l_hash_val
        from dual;
        
        begin
            -- Look for rows 
            select dim.*
                into r_dim_row
            from oie_ws.dim_oie_housing_application dim
            where dim.application_id = stg.application_id
            and dim.current_ind = 'Y';        
            
            -- Look for rows that have changed
            select count(*)
                into l_mismatched_rows_found
            from oie_ws.dim_oie_housing_application dim
            where dim.application_id = stg.application_id
            and dim.hash <> l_hash_val;
            
            -- Update any rows in the dim table for this application.
            if l_mismatched_rows_found > 0 then
                update oie_ws.dim_oie_housing_application dim
                set current_ind = 'N',
                    eff_end_dt = l_run_date
                where dim.application_id = stg.application_id
                and dim.hash <> l_hash_val
                and current_ind = 'Y';
            end if;
            
            -- Insert this row into the dim table if it's not already there.
            if l_mismatched_rows_found > 0 then
                insert into oie_ws.dim_oie_housing_application
                (
                    housing_app_key,
                    application_id,
                    emplid,
                    applicant_name,
                    residence_session,
                    student_category,
                    application_status,
                    data_origin,
                    created_ew_dttm,
                    lastupd_ew_dttm,
                    current_ind,
                    eff_start_dt,
                    eff_end_dt,
                    hash,
                    src_sys_ind
                )
                values
                (
                    seq_oie_dim_hsg_app.nextval,
                    stg.application_id,
                    stg.emplid,
                    stg.applicant_name,
                    stg.residence_session,
                    stg.student_category,
                    stg.application_status,
                    nvl(r_dim_row.data_origin, '-'),
                    nvl(r_dim_row.created_ew_dttm, systimestamp),
                    systimestamp,
                    'Y',
                    l_run_date,
                    to_date('2099-12-21', 'YYYY-MM-DD'),
                    l_hash_val,
                    nvl(r_dim_row.src_sys_ind, '-')
                );
            end if;
        exception
            when NO_DATA_FOUND then
                insert into oie_ws.dim_oie_housing_application
                    (
                        housing_app_key,
                        application_id,
                        emplid,
                        applicant_name,
                        residence_session,
                        student_category,
                        application_status,
                        data_origin,
                        created_ew_dttm,
                        lastupd_ew_dttm,
                        current_ind,
                        eff_start_dt,
                        eff_end_dt,
                        hash,
                        src_sys_ind
                    )
                    values
                    (
                        seq_oie_dim_hsg_app.nextval,
                        stg.application_id,
                        stg.emplid,
                        stg.applicant_name,
                        stg.residence_session,
                        stg.student_category,
                        stg.application_status,
                        '-',
                        systimestamp,
                        systimestamp,
                        'Y',
                        l_run_date,
                        to_date('2099-12-21', 'YYYY-MM-DD'),
                        l_hash_val,
                        '-'
                    );
                when others then
                    rollback;
                    raise;
            end;
        
    end loop;
    
    commit;
    
exception 
    when others then
        rollback;
        raise;
end;
