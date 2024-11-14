provider "aws" {
  region = "us-west-2"
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "travel-agency-data-lakes"
}


