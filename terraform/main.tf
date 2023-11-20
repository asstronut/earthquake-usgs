terraform {
  required_version = ">= 1.0"
  backend "local" {
    
  }
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.PROJECT
  region = var.REGION
}

resource "google_storage_bucket" "data-lake-bucket" {
  name = var.DATA_LAKE_BUCKET
  location = var.REGION

  storage_class = var.STORAGE_CLASS
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project = var.PROJECT
  location = var.REGION
}
