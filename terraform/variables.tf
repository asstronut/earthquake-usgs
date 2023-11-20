variable "DATA_LAKE_BUCKET" {
  description = "Name of the bucket in GCS where the raw files will be stored"
  type = string
}

variable "PROJECT" {
  description = "Global Historical Earthquake Data Engineering"
}

variable "REGION" {
  description = "Region for GCP resources"
  default = "europe-west2"
}

variable "STORAGE_CLASS" {
  description = "Storage class type for cloud storage bucket"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery dataset to store and query data"
  type = string
  default = "earthquake_usgs"
}
