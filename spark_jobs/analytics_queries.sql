-- =============================================================
-- Аналитические запросы для курсовой
-- Запуск: docker compose exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/analytics_queries.sql
-- =============================================================

USE ice;

-- ---------- 1. Распределение классов по всем кадрам ----------
-- Показывает, какой тип льда доминирует во всём датасете.
SELECT
    class_name,
    COUNT(*) AS detections,
    SUM(area_pixels) / 1e6 AS total_megapixels,
    AVG(confidence) AS avg_confidence
FROM detections
GROUP BY class_name
ORDER BY detections DESC;


-- ---------- 2. Средняя концентрация льда по часам суток ----------
-- Показывает, меняется ли видимость/концентрация в зависимости от времени.
SELECT
    HOUR(hour) AS hour_of_day,
    cam_id,
    AVG(avg_ice_conc) AS mean_conc,
    AVG(avg_severity) AS mean_severity,
    COUNT(*) AS hours_recorded
FROM hourly_summary
GROUP BY HOUR(hour), cam_id
ORDER BY cam_id, hour_of_day;


-- ---------- 3. Периоды с высокой опасностью ----------
-- Часы, когда уровень опасности был 'danger'.
SELECT
    hour,
    cam_id,
    ROUND(avg_ice_conc, 3) AS conc,
    ROUND(avg_severity, 3) AS severity,
    total_frames
FROM hourly_summary
WHERE danger_level = 'danger'
ORDER BY hour DESC
LIMIT 50;


-- ---------- 4. Корреляция появления судна с плотностью льда ----------
-- Есть ли связь: когда рядом судно, плотность льда обычно выше?
SELECT
    cam_id,
    COUNT(*) AS frames,
    AVG(ice_conc) AS mean_conc,
    AVG(ice_severity) AS mean_severity
FROM frames
GROUP BY cam_id
ORDER BY cam_id;


-- ---------- 5. Размеры детекций — процентили по классам ----------
-- Насколько крупными бывают объекты каждого класса.
SELECT
    class_name,
    PERCENTILE_APPROX(area_pixels, 0.25) AS p25,
    PERCENTILE_APPROX(area_pixels, 0.50) AS median,
    PERCENTILE_APPROX(area_pixels, 0.75) AS p75,
    PERCENTILE_APPROX(area_pixels, 0.95) AS p95,
    MAX(area_pixels) AS max_area
FROM detections
GROUP BY class_name;


-- ---------- 6. Динамика изменения концентрации по времени ----------
-- Как меняется концентрация льда в течение суток.
SELECT
    DATE(hour) AS day,
    cam_id,
    AVG(avg_ice_conc) AS daily_avg_conc,
    MAX(max_ice_conc) AS daily_peak,
    SUM(total_frames) AS total_frames
FROM hourly_summary
GROUP BY DATE(hour), cam_id
ORDER BY day DESC, cam_id;


-- ---------- 7. Сравнение двух камер ----------
-- Какая камера видит больше льда / судов?
SELECT
    cam_id,
    COUNT(*) AS total_frames,
    AVG(ice_conc) AS mean_conc,
    AVG(ice_severity) AS mean_severity
FROM frames
GROUP BY cam_id;
