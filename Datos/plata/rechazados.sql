SELECT * FROM FLOWFILE
WHERE NOT COALESCE(
  ore_tonnes > 0
  AND ag_grade_g_t > 0
  AND recovery_pct BETWEEN 0 AND 100,
  FALSE
)
