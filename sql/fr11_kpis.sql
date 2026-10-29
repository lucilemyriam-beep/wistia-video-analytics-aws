SELECT 'Media' AS metric, COUNT(*) AS total_records
FROM public.dim_media
UNION ALL
SELECT 'Visitors' AS metric, COUNT(*) AS total_records
FROM public.dim_visitor;
