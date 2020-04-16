declare
   cursor c_app_recs is (
        select distinct
            stg.residence_session residence_session,
            nvl(map.academic_year, pkg_oie_etl.NO_DATA_FOUND) academic_year,
            nvl(map.academic_term, pkg_oie_etl.NO_DATA_FOUND) academic_term
        from stg_oie_housing_apps stg
        left outer join map_oie_hsg_resid_sess map
        on stg.residence_session = map.residence_session
        where loaded <> 'Y'
   );
   
    r_dim_row oie_ws.dim_oie_hsg_resid_sess%rowtype;
   
    l_hash_val pkg_oie_etl.etl_hash_t;
   
    l_run_dttm constant timestamp := systimestamp;

    l_mismatched_rows_found number;
   
begin
    for stg in c_app_recs loop

        select standard_hash(
            stg.residence_session ||
            stg.academic_year ||
            stg.academic_term)
            into l_hash_val
        from dual;
        
        begin
            -- Look for rows 
            select dim.*
                into r_dim_row
            from oie_ws.dim_oie_hsg_resid_sess dim
            where dim.residence_session = stg.residence_session
            and dim.current_ind = 'Y';        
            
            select count(*)
                into l_mismatched_rows_found
            from oie_ws.dim_oie_hsg_resid_sess dim
            where dim.residence_session = stg.residence_session
            and dim.hash <> l_hash_val
            and current_ind = 'Y';
            
            -- Insert this row into the dim table if it's not already there.
            if l_mismatched_rows_found > 0 then
                
                update oie_ws.dim_oie_hsg_resid_sess dim
                set dim.current_ind = 'N',
                    dim.eff_end_dt = trunc(l_run_dttm)
                where dim.residence_session = stg.residence_session
                and dim.hash <> l_hash_val
                and dim.current_ind = 'Y';            
            
                insert into oie_ws.dim_oie_hsg_resid_sess
                (
                    residence_session_key,
                    residence_session,
                    academic_year,
                    academic_term,
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
                    seq_oie_hsg_dim_resid_sess.nextval,
                    stg.residence_session,
                    stg.academic_year,
                    stg.academic_term,
                    nvl(r_dim_row.data_origin, pkg_oie_etl.NO_DATA_FOUND),
                    nvl(r_dim_row.created_ew_dttm, l_run_dttm),
                    l_run_dttm,
                    'Y',
                    trunc(l_run_dttm),
                    pkg_oie_etl.END_OF_TIME,
                    l_hash_val,
                    nvl(r_dim_row.src_sys_ind, pkg_oie_etl.NO_DATA_FOUND)
                );
            end if;
        exception
            when NO_DATA_FOUND then
                insert into oie_ws.dim_oie_hsg_resid_sess
                    (
                        residence_session_key,
                        residence_session,
                        academic_year,
                        academic_term,
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
                        seq_oie_hsg_dim_resid_sess.nextval,
                        stg.residence_session,
                        stg.academic_year,
                        stg.academic_term,
                        pkg_oie_etl.NO_DATA_FOUND,
                        l_run_dttm,
                        l_run_dttm,
                        'Y',
                        trunc(l_run_dttm),
                        pkg_oie_etl.END_OF_TIME,
                        l_hash_val,
                        pkg_oie_etl.NO_DATA_FOUND
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