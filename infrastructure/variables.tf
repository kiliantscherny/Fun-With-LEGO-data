# variable "credentials" {
#   description = "My Credentials"
#   default     = "path/to/your/credentials.json"
#   ex: if you have a directory where this file is called keys with your service account json file
#   saved there as my-creds.json you could use default = "./keys/my-creds.json"
#   To set the environment variable you can use the following command in your terminal:
#   export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"
#   To check the environment variable, run: `echo $GOOGLE_APPLICATION_CREDENTIALS`
# }


variable "project" {
  description = "Project"
  # Update to your project name
  default     = "dtc-de-kilian"
}

variable "region" {
  description = "Region"
  # Update the below to your desired region
  default     = "us-east1-b"
}

variable "location" {
  description = "Project Location"
  # Update the below to your desired location
  default     = "US"
}

variable "bq_dataset_name" {
  description = "DTC DE Project Dataset"
  #Update the below to what you want your dataset to be called
  default     = "lego_raw"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  # Update the below to a unique bucket name (NOTE! This must be globally unique – nobody else in the world in any project can have the same bucket name)
  default     = "lego-bucket-dtc-de-kilian"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}