# ─────────────────────────────────────────────────────────────
# Glue Crawler (no classifier – uses default CSV handling)
# ─────────────────────────────────────────────────────────────

resource "aws_glue_crawler" "olist_raw_crawler" {
  name          = "olist-raw-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.olist_raw_db.name

  description = "Crawls raw Olist CSVs in S3 and populates Glue Data Catalog"

  s3_target {
    path = "s3://${local.bucket_name}/raw/"
  }

  # No classifiers block

  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })

  schedule = "cron(30 2 * * ? *)"
}