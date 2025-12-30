---Tổng hợp số lượng


(Data Volume)SELECT 'summary_final' as table_name, COUNT(*) as row_count FROM `glamira_bronze.summary_final`
UNION ALL
SELECT 'ip_locations' as table_name, COUNT(*) as row_count FROM `glamira_bronze.ip_locations`
UNION ALL
SELECT 'products_raw' as table_name, COUNT(*) as row_count FROM `glamira_project6.products_raw`
WHERE _PARTITIONDATE IS NOT NULL; -- Vượt qua lỗi partition filter


---Kiểm tra chất lượng (Data Quality)
SELECT
    COUNTIF(ip IS NULL) as missing_ip,           -- IP cực kỳ quan trọng để định vị
    COUNTIF(email IS NULL) as missing_email,     -- Check thông tin khách hàng
    COUNT(DISTINCT currency) as unique_currencies, -- Có đủ 85 loại tiền tệ không?
    MIN(price) as min_price,                     -- Có bị giá âm không?
    MAX(price) as max_price                      -- Giá cao nhất là bao nhiêu?
FROM `glamira_bronze.summary_final`;

---Kiểm tra tính kết nối (Relationship Check)
SELECT
    COUNT(DISTINCT s.ip) as unique_ips_in_summary,
    COUNT(DISTINCT i.ip) as unique_ips_in_location_table,
    SAFE_DIVIDE(COUNT(DISTINCT i.ip), COUNT(DISTINCT s.ip)) * 100 as match_rate
FROM `glamira_bronze.summary_final` s
LEFT JOIN `glamira_bronze.ip_locations` i ON s.ip = i.ip;