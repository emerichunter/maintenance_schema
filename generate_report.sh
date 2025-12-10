#!/bin/bash
DB_NAME=$1
OUTPUT_FILE="DB_Audit_${DB_NAME}_$(date +%Y%m%d).html"
STYLE="<style>body{font-family:'Segoe UI',sans-serif;background:#f4f4f4;padding:20px}h1{color:#2c3e50}h2{color:#34495e;border-bottom:2px solid #3498db;padding-bottom:10px}table{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 3px rgba(0,0,0,0.2)}th{background:#2980b9;color:#fff;padding:12px;text-align:left}td{padding:10px;border-bottom:1px solid #ddd}tr:nth-child(even){background:#f9f9f9}</style>"

echo "<html><head><title>Audit: $DB_NAME</title>$STYLE</head><body><h1>DB Health: $DB_NAME</h1>" > $OUTPUT_FILE
psql -d "$DB_NAME" -H -c "SELECT * FROM maintenance_schema.rpt_executive_summary" >> $OUTPUT_FILE
echo "<h2>Top Slow Queries</h2>" >> $OUTPUT_FILE
psql -d "$DB_NAME" -H -c "SELECT * FROM maintenance_schema.rpt_pss_top_time_io LIMIT 10" >> $OUTPUT_FILE
echo "<h2>Bloat & Vacuum</h2>" >> $OUTPUT_FILE
psql -d "$DB_NAME" -H -c "SELECT * FROM maintenance_schema.rpt_autovacuum_thresholds LIMIT 20" >> $OUTPUT_FILE
echo "</body></html>" >> $OUTPUT_FILE
echo "Report generated: $OUTPUT_FILE"
