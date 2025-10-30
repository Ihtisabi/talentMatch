WITH params AS (
    SELECT 'JV001' AS job_vacancy_id
),

-- STEP 1: CLEAN & NORMALIZE DATA

--Clean MBTI: I/E-N/S-F/J-J/P
clean_mbti AS (
    SELECT 
        employee_id,
        CASE WHEN UPPER(SUBSTRING(TRIM(mbti), 1, 1)) = 'E' 
             THEN 'Extraversion'
             WHEN UPPER(SUBSTRING(TRIM(mbti), 1, 1)) = 'I'
             THEN 'Introversion' END AS ei_tv,
        CASE WHEN UPPER(SUBSTRING(TRIM(mbti), 2, 1)) = 'S' 
             THEN 'Sensing'
             WHEN UPPER(SUBSTRING(TRIM(mbti), 2, 1)) = 'N'
             THEN 'Intuition' END AS sn_tv,
        CASE WHEN UPPER(SUBSTRING(TRIM(mbti), 3, 1)) = 'T' 
             THEN 'Thinking'
             WHEN UPPER(SUBSTRING(TRIM(mbti), 3, 1)) = 'F'
             THEN 'Feeling' END AS tf_tv,
        CASE WHEN UPPER(SUBSTRING(TRIM(mbti), 4, 1)) = 'J' 
             THEN 'Judging'
             WHEN UPPER(SUBSTRING(TRIM(mbti), 4, 1)) = 'P'
             THEN 'Perceiving' END AS jp_tv
    FROM profiles_psych
    WHERE mbti IS NOT NULL
      AND LENGTH(TRIM(mbti)) = 4
),

-- Clean DISC: D-I-S-C
clean_disc AS (
    SELECT 
        employee_id,
        UPPER(TRIM(disc)) AS disc_clean,
        CASE UPPER(SUBSTRING(TRIM(disc), 1, 1))
             WHEN 'D' THEN 'Dominance'
             WHEN 'I' THEN 'Influence'
             WHEN 'S' THEN 'Steadiness'
             WHEN 'C' THEN 'Compliance'
        END AS char1,

        CASE UPPER(SUBSTRING(TRIM(disc), 2, 1))
             WHEN 'D' THEN 'Dominance'
             WHEN 'I' THEN 'Influence'
             WHEN 'S' THEN 'Steadiness'
             WHEN 'C' THEN 'Compliance'
        END AS char2

    FROM profiles_psych
    WHERE disc IS NOT NULL
      AND LENGTH(TRIM(disc)) = 2
),

-- Clean Strength: Re-rank yang ada di map_tgv saja
clean_strength AS (
    SELECT 
        s.employee_id,
        s.theme,
        ROW_NUMBER() OVER (PARTITION BY s.employee_id ORDER BY s.rank) AS new_rank
    FROM strengths s
    INNER JOIN map_tgv m ON s.theme = m.sub_tv AND m.tv = 'CliftonStrengths'
),

-- Get benchmark employees
benchmark_employees AS (
    SELECT 
        tb.job_vacancy_id,
        UNNEST(tb.selected_talent_ids) AS employee_id
    FROM talent_benchmarks tb
    CROSS JOIN params p
    WHERE tb.job_vacancy_id = p.job_vacancy_id
),

-- STEP 2: NORMALIZE ALL TVs WITH MAPPING

-- Numeric TVs: IQ, GTQ, TIKI, Pauli
numeric_tvs AS (
    SELECT 
        pp.employee_id,
        m.tv,
        m.sub_tv,
        m.tgv,
        pp.iq AS tv_score,
        m.note
    FROM profiles_psych pp
    INNER JOIN map_tgv m ON m.tv = 'IQ'
    WHERE pp.iq IS NOT NULL AND pp.iq BETWEEN 80 AND 140
    
    UNION ALL
    
    SELECT 
        pp.employee_id,
        m.tv,
        m.sub_tv,
        m.tgv,
        pp.gtq AS tv_score,
        m.note
    FROM profiles_psych pp
    INNER JOIN map_tgv m ON m.tv = 'GTQ'
    WHERE pp.gtq IS NOT NULL
    
    UNION ALL
    
    SELECT 
        pp.employee_id,
        m.tv,
        m.sub_tv,
        m.tgv,
        pp.tiki AS tv_score,
        m.note
    FROM profiles_psych pp
    INNER JOIN map_tgv m ON m.tv = 'TIKI'
    WHERE pp.tiki IS NOT NULL
    
    UNION ALL
    
    SELECT 
        pp.employee_id,
        m.tv,
        m.sub_tv,
        m.tgv,
        pp.pauli AS tv_score,
        m.note
    FROM profiles_psych pp
    INNER JOIN map_tgv m ON m.tv = 'Pauli'
    WHERE pp.pauli IS NOT NULL
),

-- PAPI TVs: Join with mapping
papi_tvs AS (
    SELECT 
        ps.employee_id,
        m.tv,
        m.sub_tv,
        m.tgv,
        ps.score AS tv_score,
        m.note
    FROM papi_scores ps
    INNER JOIN map_tgv m ON ps.scale_code = m.sub_tv
    WHERE ps.score BETWEEN 1 AND 9
      AND m.tv = 'PAPI Kostick'
),

-- MBTI TVs: Unpivot with mapping
mbti_tvs AS (
    SELECT cm.employee_id,m.tv, m.sub_tv, m.tgv, cm.ei_tv AS tv_value, 1 AS pos
    FROM clean_mbti cm
    INNER JOIN map_tgv m ON cm.ei_tv = m.sub_tv
    WHERE cm.ei_tv IS NOT NULL
    
    UNION ALL
    
    SELECT cm.employee_id,m.tv, m.sub_tv, m.tgv, cm.sn_tv AS tv_value, 2 AS pos
    FROM clean_mbti cm
    INNER JOIN map_tgv m ON cm.sn_tv = m.sub_tv
    WHERE cm.sn_tv IS NOT NULL
    
    UNION ALL
    
    SELECT cm.employee_id,m.tv, m.sub_tv, m.tgv, cm.tf_tv AS tv_value, 3 AS pos
    FROM clean_mbti cm
    INNER JOIN map_tgv m ON cm.tf_tv = m.sub_tv
    WHERE cm.tf_tv IS NOT NULL
    
    UNION ALL
    
    SELECT cm.employee_id,m.tv, m.sub_tv, m.tgv, cm.jp_tv AS tv_value, 4 AS pos
    FROM clean_mbti cm
    INNER JOIN map_tgv m ON cm.jp_tv = m.sub_tv
    WHERE cm.jp_tv IS NOT NULL
),

-- DISC TVs: Unpivot each character (exactly like MBTI structure)
disc_tvs AS (
    SELECT 
        cd.employee_id,
        m.tv,
        m.sub_tv,
        m.tgv,
        cd.char1 as tv_value,
        1 AS pos
    FROM clean_disc cd
    INNER JOIN map_tgv m ON cd.char1 = m.sub_tv
    WHERE cd.char1 IS NOT NULL
    
    UNION ALL
    
    -- Second character
    SELECT 
        cd.employee_id,
        m.tv,
        m.sub_tv,
        m.tgv,
        cd.char2 as tv_value,
        2 AS pos
    FROM clean_disc cd
    INNER JOIN map_tgv m ON cd.char2 = m.sub_tv
    WHERE cd.char2 IS NOT NULL
),

-- CliftonStrengths TVs: Join with mapping (menggunakan clean_strength yang sudah re-rank)
strengths_tvs AS (
    SELECT 
        cs.employee_id,
        m.tv,
        m.sub_tv,
        m.tgv,
        cs.theme AS tv_value,
        cs.new_rank AS pos
    FROM clean_strength cs
    INNER JOIN map_tgv m ON cs.theme = m.sub_tv
    WHERE cs.new_rank <= 5  -- Top 5 setelah re-ranking
      AND m.tv = 'CliftonStrengths'
),

-- Aggregate top 5 strengths per employee untuk array matching
strengths_array AS (
    SELECT 
        employee_id,
        ARRAY_AGG(theme ORDER BY new_rank) AS top5_themes
    FROM clean_strength
    WHERE new_rank <= 5
    GROUP BY employee_id
),

-- STEP 3: COMPUTE BASELINES

numeric_baselines AS (
    SELECT 
        ntv.tv,
        ntv.sub_tv,
        ntv.tgv,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ntv.tv_score) AS baseline_score,
        ntv.note
    FROM numeric_tvs ntv
    INNER JOIN benchmark_employees be ON ntv.employee_id = be.employee_id
    GROUP BY ntv.tv, ntv.sub_tv, ntv.tgv, ntv.note
    
    UNION ALL
    --papi
    SELECT 
        ptv.tv,
        ptv.sub_tv,
        ptv.tgv,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ptv.tv_score) AS baseline_score,
        ptv.note
    FROM papi_tvs ptv
    INNER JOIN benchmark_employees be ON ptv.employee_id = be.employee_id
    GROUP BY ptv.tv, ptv.sub_tv, ptv.tgv, ptv.note
),

categorical_baselines AS (
    SELECT 
        mtv.tv,
        mtv.sub_tv,
        mtv.tgv,
        mtv.pos,
        MODE() WITHIN GROUP (ORDER BY mtv.tv_value) AS baseline_value
    FROM mbti_tvs mtv
    INNER JOIN benchmark_employees be ON mtv.employee_id = be.employee_id
    GROUP BY mtv.tv, mtv.sub_tv, mtv.tgv, mtv.pos
    
    UNION ALL
    
    SELECT 
        dtv.tv,
        dtv.sub_tv,
        dtv.tgv,
        dtv.pos,
        MODE() WITHIN GROUP (ORDER BY dtv.tv_value) AS baseline_value
    FROM disc_tvs dtv
    INNER JOIN benchmark_employees be ON dtv.employee_id = be.employee_id
    GROUP BY dtv.tv,dtv.sub_tv, dtv.tgv, dtv.pos
    
    UNION ALL
    
    SELECT 
        stv.tv,
        stv.sub_tv,
        stv.tgv,
        stv.pos,
        MODE() WITHIN GROUP (ORDER BY stv.tv_value) AS baseline_value
    FROM strengths_tvs stv
    INNER JOIN benchmark_employees be ON stv.employee_id = be.employee_id
    GROUP BY stv.tv,stv.sub_tv, stv.tgv, stv.pos
),

-- STEP 4: CALCULATE TV MATCH RATES

numeric_tv_matches AS (
    SELECT 
        ntv.employee_id,
        ntv.tgv,
        ntv.tv,
        ntv.sub_tv,
        nb.baseline_score,
        ntv.tv_score AS user_score,
        CASE 
            WHEN LOWER(TRIM(nb.note)) = 'inverse scale' THEN 
                GREATEST(0, LEAST(
                    ((2 * nb.baseline_score - ntv.tv_score) / NULLIF(nb.baseline_score, 0)) * 100, 
                    100
                ))
            ELSE 
                GREATEST(0, LEAST(
                    (ntv.tv_score / NULLIF(nb.baseline_score, 0)) * 100, 
                    100
                ))
        END AS tv_match_rate
    FROM numeric_tvs ntv
    INNER JOIN numeric_baselines nb ON ntv.tv = nb.tv
    
    UNION ALL
    
    SELECT 
        ptv.employee_id,
        ptv.tgv,
        ptv.tv,
        ptv.sub_tv,
        pb.baseline_score,
        ptv.tv_score AS user_score,
        CASE 
            WHEN LOWER(TRIM(pb.note)) = 'inverse scale' THEN 
                GREATEST(0, LEAST(
                    ((2 * pb.baseline_score - ptv.tv_score) / NULLIF(pb.baseline_score, 0)) * 100, 
                    100
                ))
            ELSE 
                GREATEST(0, LEAST(
                    (ptv.tv_score / NULLIF(pb.baseline_score, 0)) * 100, 
                    100
                ))
        END AS tv_match_rate
    FROM papi_tvs ptv
    INNER JOIN numeric_baselines pb ON ptv.sub_tv = pb.sub_tv
),

categorical_tv_matches AS (
    SELECT 
        mtv.employee_id,
        mtv.tgv,
        mtv.tv,
        mtv.sub_tv,
        mtv.pos,
        cb.baseline_value AS baseline_score,
        mtv.tv_value AS user_score,
        CASE WHEN mtv.tv_value = cb.baseline_value THEN 100.0 ELSE 0.0 END AS tv_match_rate
    FROM mbti_tvs mtv
    INNER JOIN categorical_baselines cb ON mtv.tv = cb.tv AND mtv.pos = cb.pos
    
    UNION ALL
    
    SELECT 
        dtv.employee_id,
        dtv.tgv,
        dtv.tv,
        dtv.sub_tv,
        dtv.pos,
        cb.baseline_value AS baseline_score,
        dtv.tv_value AS user_score,
        CASE WHEN dtv.tv_value = cb.baseline_value THEN 100.0 ELSE 0.0 END AS tv_match_rate
    FROM disc_tvs dtv
    INNER JOIN categorical_baselines cb ON dtv.tv = cb.tv AND dtv.pos = cb.pos
    
    UNION ALL
    
    -- Strengths matching dengan array
    SELECT 
        sa.employee_id,
        cb.tgv,
        cb.tv,
        cb.sub_tv,
        cb.pos,
        cb.baseline_value AS baseline_score,
        ARRAY_TO_STRING(sa.top5_themes, ', ') AS user_score,
        CASE WHEN cb.baseline_value = ANY(sa.top5_themes) THEN 100.0 ELSE 0.0 END AS tv_match_rate
    FROM strengths_array sa
    CROSS JOIN categorical_baselines cb
    WHERE cb.tv = 'CliftonStrengths'
),

all_tv_matches AS (
    SELECT 
        employee_id,
        tgv,
        tv,
        sub_tv,
        CAST(baseline_score AS TEXT) AS baseline_score,  -- Cast numeric → text
        CAST(user_score AS TEXT) AS user_score,          -- Cast numeric → text
        tv_match_rate                                     -- Tetap numeric ✅
    FROM numeric_tv_matches
    
    UNION ALL
    
    SELECT 
        employee_id,
        tgv,
        tv,
        sub_tv,
        baseline_score,    -- Sudah text
        user_score,        -- Sudah text (atau array string untuk strengths)
        tv_match_rate      -- Sudah numeric ✅
    FROM categorical_tv_matches
),

-- STEP 5: AGGREGATE TO TGV & FINAL LEVEL

-- Count total available TVs per TGV (from baselines that were actually formed)
total_tvs_per_tgv AS (
    SELECT 
        tgv,
        COUNT(DISTINCT tv) AS total_tv_count
    FROM (
        SELECT tgv, tv FROM numeric_baselines
        UNION
        SELECT tgv, tv FROM categorical_baselines
    ) all_baselines
    GROUP BY tgv
),

-- Count filled TVs per employee per TGV
filled_tvs_per_employee_tgv AS (
    SELECT 
        employee_id,
        tgv,
        COUNT(DISTINCT tv) AS filled_tv_count
    FROM all_tv_matches
    GROUP BY employee_id, tgv
),



tgv_matches AS (
    SELECT 
        atm.employee_id,
        atm.tgv,
        AVG(atm.tv_match_rate)::NUMERIC AS tgv_match_rate,
        ftv.filled_tv_count,
        ttv.total_tv_count
    FROM all_tv_matches atm
    INNER JOIN filled_tvs_per_employee_tgv ftv 
        ON atm.employee_id = ftv.employee_id AND atm.tgv = ftv.tgv
    INNER JOIN total_tvs_per_tgv ttv ON atm.tgv = ttv.tgv
    GROUP BY atm.employee_id, atm.tgv, ftv.filled_tv_count,ttv.total_tv_count
),

-- Count total available TGVs
total_tgvs AS (
    SELECT COUNT(DISTINCT tgv) AS total_tgv_count
    FROM map_tgv
),

-- Count filled TGVs per employee
filled_tgvs_per_employee AS (
    SELECT 
        employee_id,
        COUNT(DISTINCT tgv) AS filled_tgv_count
    FROM all_tv_matches
    GROUP BY employee_id
),


final_matches AS (
    SELECT 
        tgv.employee_id,
        AVG(tgv.tgv_match_rate)::NUMERIC AS final_match_rate,
        ftgv.filled_tgv_count,
        ttgv.total_tgv_count
    FROM tgv_matches tgv
    CROSS JOIN total_tgvs ttgv
    INNER JOIN filled_tgvs_per_employee ftgv ON tgv.employee_id = ftgv.employee_id
    GROUP BY tgv.employee_id, ftgv.filled_tgv_count,
        ttgv.total_tgv_count
),

-- STEP 6: FORMAT OUTPUT

final_output AS (
    SELECT 
        e.employee_id,
        e.fullname,
        dd.name AS directorate,
        dp.name AS role,
        dg.name AS grade,
        atv.tgv,
        atv.tv,
        atv.sub_tv,
        atv.baseline_score,
        atv.user_score,
        ROUND(atv.tv_match_rate::NUMERIC, 2) AS tv_match_rate,
        ROUND(tgv.tgv_match_rate, 2) AS tgv_match_rate,
        ROUND(fm.final_match_rate, 2) AS final_match_rate,
        fm.filled_tgv_count || '/' || fm.total_tgv_count AS final_data_filled
    FROM all_tv_matches atv
    INNER JOIN employees e ON atv.employee_id = e.employee_id
    INNER JOIN tgv_matches tgv 
        ON atv.employee_id = tgv.employee_id AND atv.tgv = tgv.tgv
    INNER JOIN final_matches fm ON atv.employee_id = fm.employee_id
    LEFT JOIN dim_directorates dd ON e.directorate_id = dd.directorate_id
    LEFT JOIN dim_positions dp ON e.position_id = dp.position_id
    LEFT JOIN dim_grades dg ON e.grade_id = dg.grade_id
)

SELECT *
FROM final_output
ORDER BY final_match_rate DESC, employee_id, tgv, sub_tv;