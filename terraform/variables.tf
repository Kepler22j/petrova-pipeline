# ============================================================
# PETROVA – Terraform Variables
# ============================================================

variable "environment" {
  description = "Deployment environment (dev / prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Environment must be 'dev' or 'prod'."
  }
}

variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
  sensitive   = true
}

variable "azure_tenant_id" {
  description = "Azure AD tenant ID"
  type        = string
  sensitive   = true
}

variable "azure_region" {
  description = "Azure region for resources"
  type        = string
  default     = "eastus2"
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_username" {
  description = "Snowflake admin username"
  type        = string
  sensitive   = true
}

variable "snowflake_password" {
  description = "Snowflake admin password"
  type        = string
  sensitive   = true
}

variable "common_tags" {
  description = "Common resource tags"
  type        = map(string)
  default = {
    project     = "PETROVA-300K"
    managed_by  = "terraform"
    cost_center = "data-engineering"
  }
}
