--	Number of titles released per year.
SELECT m.releaseYear, COUNT(*) AS Movies 
FROM gold_catalog.movies.movies m
GROUP BY m.releaseYear
ORDER BY 1 DESC;


-- Top directors by content production (count)
SELECT m.director, COUNT(*) AS Movies 
FROM gold_catalog.movies.movies m
WHERE director IS NOT NULL
GROUP BY m.director
ORDER BY 2 DESC
LIMIT 10;

-- Most common age certifications
SELECT m.ageCertification, COUNT(*) AS Movies 
FROM gold_catalog.movies.movies m
WHERE ageCertification IS NOT NULL
GROUP BY m.ageCertification
ORDER BY 2 DESC
LIMIT 10;
