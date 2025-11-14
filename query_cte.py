def generate_cte_query(job_vacancy_id: str, selected_ids: list[str]) -> str:
    """
    Generate SQL query untuk baseline & match calculation
    dengan parameter dinamis dari user (tanpa ubah DB utama).
    """

    # Convert list ke format PostgreSQL array syntax
    selected_ids_str = ",".join([f"'{i}'" for i in selected_ids])

    return f"""
    with
        params as (
            select '{job_vacancy_id}' as job_vacancy_id
        ),
        -- STEP 1: CLEAN & NORMALIZE DATA
        --Clean MBTI: I/E-N/S-F/J-J/P
        clean_mbti as (
            select
            employee_id,
            case
                when UPPER(SUBSTRING(TRIM(mbti), 1, 1)) = 'E' then 'Extraversion'
                when UPPER(SUBSTRING(TRIM(mbti), 1, 1)) = 'I' then 'Introversion'
            end as ei_tv,
            case
                when UPPER(SUBSTRING(TRIM(mbti), 2, 1)) = 'S' then 'Sensing'
                when UPPER(SUBSTRING(TRIM(mbti), 2, 1)) = 'N' then 'Intuition'
            end as sn_tv,
            case
                when UPPER(SUBSTRING(TRIM(mbti), 3, 1)) = 'T' then 'Thinking'
                when UPPER(SUBSTRING(TRIM(mbti), 3, 1)) = 'F' then 'Feeling'
            end as tf_tv,
            case
                when UPPER(SUBSTRING(TRIM(mbti), 4, 1)) = 'J' then 'Judging'
                when UPPER(SUBSTRING(TRIM(mbti), 4, 1)) = 'P' then 'Perceiving'
            end as jp_tv
            from
            profiles_psych
            where
            mbti is not null
            and LENGTH(TRIM(mbti)) = 4
        ),
        -- Clean DISC: D-I-S-C
        clean_disc as (
            select
            employee_id,
            UPPER(TRIM(disc)) as disc_clean,
            case UPPER(SUBSTRING(TRIM(disc), 1, 1))
                when 'D' then 'Dominance'
                when 'I' then 'Influence'
                when 'S' then 'Steadiness'
                when 'C' then 'Compliance'
            end as char1,
            case UPPER(SUBSTRING(TRIM(disc), 2, 1))
                when 'D' then 'Dominance'
                when 'I' then 'Influence'
                when 'S' then 'Steadiness'
                when 'C' then 'Compliance'
            end as char2
            from
            profiles_psych
            where
            disc is not null
            and LENGTH(TRIM(disc)) = 2
        ),
        -- Clean Strength: Re-rank yang ada di map_tgv saja
        clean_strength as (
            select
            s.employee_id,
            s.theme,
            ROW_NUMBER() over (
                partition by
                s.employee_id
                order by
                s.rank
            ) as new_rank
            from
            strengths s
            inner join map_tgv m on s.theme = m.sub_tv
            and m.tv = 'CliftonStrengths'
        ),
        benchmark_employees as (
            select
            p.job_vacancy_id,
            unnest(array[{selected_ids_str}]) as bench_employee_id
            from params p
        ),
        -- STEP 2: NORMALIZE ALL TVs WITH MAPPING
        -- Numeric TVs: IQ, GTQ, TIKI, Pauli
        numeric_tvs as (
            select
            pp.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            pp.iq as tv_score,
            m.note
            from
            profiles_psych pp
            inner join map_tgv m on m.tv = 'IQ'
            where
            pp.iq is not null
            and pp.iq between 80 and 140
            union all
            select
            pp.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            pp.gtq as tv_score,
            m.note
            from
            profiles_psych pp
            inner join map_tgv m on m.tv = 'GTQ'
            where
            pp.gtq is not null
            union all
            select
            pp.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            pp.tiki as tv_score,
            m.note
            from
            profiles_psych pp
            inner join map_tgv m on m.tv = 'TIKI'
            where
            pp.tiki is not null
            union all
            select
            pp.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            pp.pauli as tv_score,
            m.note
            from
            profiles_psych pp
            inner join map_tgv m on m.tv = 'Pauli'
            where
            pp.pauli is not null
        ),
        -- PAPI TVs: Join with mapping
        papi_tvs as (
            select
            ps.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            ps.score as tv_score,
            m.note
            from
            papi_scores ps
            inner join map_tgv m on ps.scale_code = m.sub_tv
            where
            ps.score between 1 and 9
            and m.tv = 'PAPI Kostick'
        ),
        -- MBTI TVs: Unpivot with mapping
        mbti_tvs as (
            select
            cm.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            cm.ei_tv as tv_value,
            1 as pos
            from
            clean_mbti cm
            inner join map_tgv m on cm.ei_tv = m.sub_tv
            where
            cm.ei_tv is not null
            union all
            select
            cm.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            cm.sn_tv as tv_value,
            2 as pos
            from
            clean_mbti cm
            inner join map_tgv m on cm.sn_tv = m.sub_tv
            where
            cm.sn_tv is not null
            union all
            select
            cm.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            cm.tf_tv as tv_value,
            3 as pos
            from
            clean_mbti cm
            inner join map_tgv m on cm.tf_tv = m.sub_tv
            where
            cm.tf_tv is not null
            union all
            select
            cm.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            cm.jp_tv as tv_value,
            4 as pos
            from
            clean_mbti cm
            inner join map_tgv m on cm.jp_tv = m.sub_tv
            where
            cm.jp_tv is not null
        ),
        -- DISC TVs: Unpivot each character (exactly like MBTI structure)
        disc_tvs as (
            select
            cd.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            cd.char1 as tv_value,
            1 as pos
            from
            clean_disc cd
            inner join map_tgv m on cd.char1 = m.sub_tv
            where
            cd.char1 is not null
            union all
            -- Second character
            select
            cd.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            cd.char2 as tv_value,
            2 as pos
            from
            clean_disc cd
            inner join map_tgv m on cd.char2 = m.sub_tv
            where
            cd.char2 is not null
        ),
        -- CliftonStrengths TVs: Join with mapping (menggunakan clean_strength yang sudah re-rank)
        strengths_tvs as (
            select
            cs.employee_id,
            m.tv,
            m.sub_tv,
            m.tgv,
            cs.theme as tv_value,
            cs.new_rank as pos
            from
            clean_strength cs
            inner join map_tgv m on cs.theme = m.sub_tv
            where
            cs.new_rank <= 5 -- Top 5 setelah re-ranking
            and m.tv = 'CliftonStrengths'
        ),
        -- Aggregate top 5 strengths per employee untuk array matching
        strengths_array as (
            select
            employee_id,
            ARRAY_AGG(
                theme
                order by
                new_rank
            ) as top5_themes
            from
            clean_strength
            where
            new_rank <= 5
            group by
            employee_id
        ),
        -- STEP 3: COMPUTE BASELINES
        numeric_baselines as (
            select
            ntv.tv,
            ntv.sub_tv,
            ntv.tgv,
            PERCENTILE_CONT(0.5) within group (
                order by
                ntv.tv_score
            ) as baseline_score,
            ntv.note
            from
            numeric_tvs ntv
            inner join benchmark_employees be on ntv.employee_id = be.bench_employee_id
            group by
            ntv.tv,
            ntv.sub_tv,
            ntv.tgv,
            ntv.note
            union all
            --papi
            select
            ptv.tv,
            ptv.sub_tv,
            ptv.tgv,
            PERCENTILE_CONT(0.5) within group (
                order by
                ptv.tv_score
            ) as baseline_score,
            ptv.note
            from
            papi_tvs ptv
            inner join benchmark_employees be on ptv.employee_id = be.bench_employee_id
            group by
            ptv.tv,
            ptv.sub_tv,
            ptv.tgv,
            ptv.note
        ),
        categorical_baselines as (
            select
            mtv.tv,
            mtv.sub_tv,
            mtv.tgv,
            mtv.pos,
            MODE() within group (
                order by
                mtv.tv_value
            ) as baseline_value
            from
            mbti_tvs mtv
            inner join benchmark_employees be on mtv.employee_id = be.bench_employee_id
            group by
            mtv.tv,
            mtv.sub_tv,
            mtv.tgv,
            mtv.pos
            union all
            select
            dtv.tv,
            dtv.sub_tv,
            dtv.tgv,
            dtv.pos,
            MODE() within group (
                order by
                dtv.tv_value
            ) as baseline_value
            from
            disc_tvs dtv
            inner join benchmark_employees be on dtv.employee_id = be.bench_employee_id
            group by
            dtv.tv,
            dtv.sub_tv,
            dtv.tgv,
            dtv.pos
            union all
            select
            stv.tv,
            stv.sub_tv,
            stv.tgv,
            stv.pos,
            MODE() within group (
                order by
                stv.tv_value
            ) as baseline_value
            from
            strengths_tvs stv
            inner join benchmark_employees be on stv.employee_id = be.bench_employee_id
            group by
            stv.tv,
            stv.sub_tv,
            stv.tgv,
            stv.pos
        ),
        -- STEP 4: CALCULATE TV MATCH RATES
        numeric_tv_matches as (
            select
            ntv.employee_id,
            ntv.tgv,
            ntv.tv,
            ntv.sub_tv,
            nb.baseline_score,
            ntv.tv_score as user_score,
            case
                when LOWER(TRIM(nb.note)) = 'inverse scale' then GREATEST(
                0,
                LEAST(
                    (
                    (2 * nb.baseline_score - ntv.tv_score) / NULLIF(nb.baseline_score, 0)
                    ) * 100,
                    100
                )
                )
                else GREATEST(
                0,
                LEAST(
                    (ntv.tv_score / NULLIF(nb.baseline_score, 0)) * 100,
                    100
                )
                )
            end as tv_match_rate
            from
            numeric_tvs ntv
            inner join numeric_baselines nb on ntv.tv = nb.tv
            union all
            select
            ptv.employee_id,
            ptv.tgv,
            ptv.tv,
            ptv.sub_tv,
            pb.baseline_score,
            ptv.tv_score as user_score,
            case
                when LOWER(TRIM(pb.note)) = 'inverse scale' then GREATEST(
                0,
                LEAST(
                    (
                    (2 * pb.baseline_score - ptv.tv_score) / NULLIF(pb.baseline_score, 0)
                    ) * 100,
                    100
                )
                )
                else GREATEST(
                0,
                LEAST(
                    (ptv.tv_score / NULLIF(pb.baseline_score, 0)) * 100,
                    100
                )
                )
            end as tv_match_rate
            from
            papi_tvs ptv
            inner join numeric_baselines pb on ptv.sub_tv = pb.sub_tv
        ),
        categorical_tv_matches as (
            select
            mtv.employee_id,
            mtv.tgv,
            mtv.tv,
            mtv.sub_tv,
            mtv.pos,
            cb.baseline_value as baseline_score,
            mtv.tv_value as user_score,
            case
                when mtv.tv_value = cb.baseline_value then 100.0
                else 0.0
            end as tv_match_rate
            from
            mbti_tvs mtv
            inner join categorical_baselines cb on mtv.tv = cb.tv
            and mtv.pos = cb.pos
            union all
            select
            dtv.employee_id,
            dtv.tgv,
            dtv.tv,
            dtv.sub_tv,
            dtv.pos,
            cb.baseline_value as baseline_score,
            dtv.tv_value as user_score,
            case
                when dtv.tv_value = cb.baseline_value then 100.0
                else 0.0
            end as tv_match_rate
            from
            disc_tvs dtv
            inner join categorical_baselines cb on dtv.tv = cb.tv
            and dtv.pos = cb.pos
            union all
            -- Strengths matching dengan array
            select
            sa.employee_id,
            cb.tgv,
            cb.tv,
            cb.sub_tv,
            cb.pos,
            cb.baseline_value as baseline_score,
            ARRAY_TO_STRING(sa.top5_themes, ', ') as user_score,
            case
                when cb.baseline_value = any (sa.top5_themes) then 100.0
                else 0.0
            end as tv_match_rate
            from
            strengths_array sa
            cross join categorical_baselines cb
            where
            cb.tv = 'CliftonStrengths'
        ),
        all_tv_matches as (
            select
            employee_id,
            tgv,
            tv,
            sub_tv,
            CAST(baseline_score as TEXT) as baseline_score, -- Cast numeric → text
            CAST(user_score as TEXT) as user_score, -- Cast numeric → text
            tv_match_rate -- Tetap numeric ✅
            from
            numeric_tv_matches
            union all
            select
            employee_id,
            tgv,
            tv,
            sub_tv,
            baseline_score, -- Sudah text
            user_score, -- Sudah text (atau array string untuk strengths)
            tv_match_rate -- Sudah numeric ✅
            from
            categorical_tv_matches
        ),
        -- STEP 5: AGGREGATE TO TGV & FINAL LEVEL
        -- Count total available TVs per TGV (from baselines that were actually formed)
        total_tvs_per_tgv as (
            select
            tgv,
            COUNT(distinct tv) as total_tv_count
            from
            (
                select
                tgv,
                tv
                from
                numeric_baselines
                union
                select
                tgv,
                tv
                from
                categorical_baselines
            ) all_baselines
            group by
            tgv
        ),
        -- Count filled TVs per employee per TGV
        filled_tvs_per_employee_tgv as (
            select
            employee_id,
            tgv,
            COUNT(distinct tv) as filled_tv_count
            from
            all_tv_matches
            group by
            employee_id,
            tgv
        ),
        tgv_matches as (
            select
            atm.employee_id,
            atm.tgv,
            AVG(atm.tv_match_rate)::NUMERIC as tgv_match_rate,
            ftv.filled_tv_count,
            ttv.total_tv_count
            from
            all_tv_matches atm
            inner join filled_tvs_per_employee_tgv ftv on atm.employee_id = ftv.employee_id
            and atm.tgv = ftv.tgv
            inner join total_tvs_per_tgv ttv on atm.tgv = ttv.tgv
            group by
            atm.employee_id,
            atm.tgv,
            ftv.filled_tv_count,
            ttv.total_tv_count
        ),
        -- Count total available TGVs
        total_tgvs as (
            select
            COUNT(distinct tgv) as total_tgv_count
            from
            map_tgv
        ),
        -- Count filled TGVs per employee
        filled_tgvs_per_employee as (
            select
            employee_id,
            COUNT(distinct tgv) as filled_tgv_count
            from
            all_tv_matches
            group by
            employee_id
        ),
        final_matches as (
            select
            tgv.employee_id,
            AVG(tgv.tgv_match_rate)::NUMERIC as final_match_rate,
            ftgv.filled_tgv_count,
            ttgv.total_tgv_count
            from
            tgv_matches tgv
            cross join total_tgvs ttgv
            inner join filled_tgvs_per_employee ftgv on tgv.employee_id = ftgv.employee_id
            group by
            tgv.employee_id,
            ftgv.filled_tgv_count,
            ttgv.total_tgv_count
        ),
        -- STEP 6: FORMAT OUTPUT
        final_output as (
            select
            e.employee_id,
            e.fullname,
            ed.name as education,
            ar.name as area,
            atv.tgv,
            atv.tv,
            atv.sub_tv,
            atv.baseline_score,
            atv.user_score,
            ROUND(atv.tv_match_rate::NUMERIC, 2) as tv_match_rate,
            ROUND(tgv.tgv_match_rate, 2) as tgv_match_rate,
            ROUND(fm.final_match_rate, 2) as match_rate,
            fm.filled_tgv_count || '/' || fm.total_tgv_count as final_data_filled
            from
            all_tv_matches atv
            inner join employees e on atv.employee_id = e.employee_id
            inner join tgv_matches tgv on atv.employee_id = tgv.employee_id
            and atv.tgv = tgv.tgv
            inner join final_matches fm on atv.employee_id = fm.employee_id
            left join dim_education ed on e.education_id = ed.education_id
            left join dim_areas ar on e.area_id = ar.area_id
        )
        
        select *
        from (
            SELECT DISTINCT ON (employee_id)
                employee_id,
                fullname,
                match_rate,
                MAX(user_score) FILTER (WHERE tv = 'CliftonStrengths') OVER (PARTITION BY employee_id) AS strength,
                FIRST_VALUE(tgv) OVER (PARTITION BY employee_id ORDER BY tgv_match_rate DESC) AS top_tgv,
                education,
                area
            FROM
                final_output
            WHERE
                match_rate >= 70
            GROUP BY
                employee_id, fullname, match_rate, education, area, tgv_match_rate, tgv, user_score, tv
            ORDER BY
                employee_id
            ) ranked
        ORDER BY match_rate DESC

    """