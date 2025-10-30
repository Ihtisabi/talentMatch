CREATE TABLE IF NOT EXISTS talent_benchmarks (
    job_vacancy_id TEXT PRIMARY KEY,
    role_name TEXT NOT NULL,
    job_level TEXT,
    role_purpose TEXT,
    selected_talent_ids TEXT[], -- Auto-populated dari employees max rating
    weights_config JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO talent_benchmarks (
    job_vacancy_id, 
    role_name, 
    job_level, 
    role_purpose, 
    selected_talent_ids,
    weights_config
)
SELECT 
    'JV001' AS job_vacancy_id,
    'Data Analyst' AS role_name,
    'Senior' AS job_level,
    'Lead data analysis projects and provide insights' AS role_purpose,
    ARRAY_AGG(e.employee_id) AS selected_talent_ids,
    NULL AS weights_config
FROM employees e
INNER JOIN dim_positions dp ON e.position_id = dp.position_id
INNER JOIN performance_yearly ep ON e.employee_id = ep.employee_id
WHERE dp.name = 'Data Analyst'
  AND ep.year = 2025
  AND ep.rating = (
      SELECT MAX(ep2.rating)
      FROM employees e2
      INNER JOIN dim_positions dp2 ON e2.position_id = dp2.position_id
      INNER JOIN performance_yearly ep2 ON e2.employee_id = ep2.employee_id
      WHERE dp2.name = 'Data Analyst'
        AND ep2.year = 2025
  )
GROUP BY dp.name
ON CONFLICT (job_vacancy_id) DO UPDATE
SET selected_talent_ids = EXCLUDED.selected_talent_ids,
    role_name = EXCLUDED.role_name;
