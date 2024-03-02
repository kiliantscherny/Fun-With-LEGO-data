terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  # If the credentials are not set as an environment variable, you can set them here
  # credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# output "credentials_path" {
#   value = var.credentials
# }

resource "google_storage_bucket" "dtc-project-bucket" {
  # project       = var.project
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "project-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}