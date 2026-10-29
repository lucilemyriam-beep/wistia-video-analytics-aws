SELECT 'dim_media' AS table_name, COUNT(*) AS total_records FROM public.dim_media
UNION ALL SELECT 'dim_visitor', COUNT(*) FROM public.dim_visitor
UNION ALL SELECT 'fact_media_engagement', COUNT(*) FROM public.fact_media_engagement
UNION ALL SELECT 'fact_media_engagement_new', COUNT(*) FROM public.fact_media_engagement_new;
