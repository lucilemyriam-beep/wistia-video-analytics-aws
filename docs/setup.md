# AWS setup and deployment instructions

1. Select AWS region `us-east-1`.
2. Create an AWS Secrets Manager secret `wistia-video-analytics/api-token` containing JSON key `WISTIA_API_TOKEN`. Never commit its value.
3. Configure Lambda `wistia-video-analytics-ingestion` with permission to read the secret and write to the Bronze S3 location. Review the `lambda/` variants and identify the one matching the **currently deployed** function.
4. Configure the S3 Bronze and Silver paths, then deploy the PySpark script `glue/wistia_bronze_to_silver.py` to the AWS Glue job `wistia-bronze-to-silver`. Verify that the deployed script matches the repository version and review the job's run history.
5. Configure Redshift Serverless and its S3 access IAM role. Add the actual DDL and COPY statements under `sql/`.
6. Configure Step Functions and EventBridge for daily runs, CloudWatch logging and alarms. Add state machine definition and schedule evidence.
7. Verify seven **consecutive** successful production days with timestamps/logs before claiming FR8 complete.
8. Run `sql/validation.sql` and `sql/fr11_kpis.sql` in Redshift Query Editor v2.
9. Open the saved FR11 chart in Redshift; its screenshot is at `dashboard/redshift_fr11_kpi_chart.png`.
10. Use GitHub Actions for CI validation; deployment automation and AWS OIDC integration require additional setup and proof before claiming automatic deployment.
