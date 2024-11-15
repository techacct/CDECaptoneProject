provider "aws" {
  region = "us-west-2"
}

# S3 Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = "travel-agency-data-lakes"
  acl    = "private"
}

# DynamoDB Table
resource "aws_dynamodb_table" "analytics_table" {
  name         = "analytics_data"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S" # String type
  }
}

# IAM Role for Lambda/ETL script
resource "aws_iam_role" "etl_role" {
  name = "etl-script-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

# IAM Policy for S3 and DynamoDB access
resource "aws_iam_policy" "etl_policy" {
  name = "etl-script-policy"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "dynamodb:PutItem"
      ],
      "Resource": [
        "${aws_s3_bucket.data_lake.arn}/*",
        "${aws_dynamodb_table.analytics_table.arn}"
      ]
    }
  ]
}
EOF
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "attach_etl_policy" {
  role       = aws_iam_role.etl_role.name
  policy_arn = aws_iam_policy.etl_policy.arn
}
