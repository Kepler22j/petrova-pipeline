# ============================================================
# PETROVA 300K – Terraform Infrastructure
# Manages: Azure resources + Snowflake objects
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.76"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }

  backend "azurerm" {
    resource_group_name  = "petrova-tfstate-rg"
    storage_account_name = "petrovaterraform"
    container_name       = "tfstate"
    key                  = "petrova-pipeline.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
}

provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_username
  password = var.snowflake_password
  role     = "PETROVA_ADMIN"
}

# ─── Azure Resources ───

resource "azurerm_resource_group" "petrova" {
  name     = "petrova-${var.environment}-rg"
  location = var.azure_region
  tags     = var.common_tags
}

resource "azurerm_storage_account" "data_lake" {
  name                     = "petrova${var.environment}lake"
  resource_group_name      = azurerm_resource_group.petrova.name
  location                 = azurerm_resource_group.petrova.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true  # Data Lake Gen2
  tags                     = var.common_tags
}

resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "archive" {
  name                  = "archive"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_data_factory" "petrova" {
  name                = "petrova-${var.environment}-adf"
  location            = azurerm_resource_group.petrova.location
  resource_group_name = azurerm_resource_group.petrova.name
  tags                = var.common_tags
}

resource "azurerm_key_vault" "petrova" {
  name                = "petrova-${var.environment}-kv"
  location            = azurerm_resource_group.petrova.location
  resource_group_name = azurerm_resource_group.petrova.name
  tenant_id           = var.azure_tenant_id
  sku_name            = "standard"
  tags                = var.common_tags
}

# ─── Snowflake Resources ───

resource "snowflake_database" "prod" {
  name    = "PETROVA_PROD"
  comment = "PETROVA 300K production database"
}

resource "snowflake_schema" "bronze" {
  database = snowflake_database.prod.name
  name     = "BRONZE"
  comment  = "Raw/immutable ingestion layer"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.prod.name
  name     = "SILVER"
  comment  = "Cleaned, validated, SCD2 layer"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.prod.name
  name     = "GOLD"
  comment  = "Business-ready, RBAC-protected layer"
}

resource "snowflake_warehouse" "etl" {
  name           = "PETROVA_ETL_WH"
  warehouse_size = var.environment == "prod" ? "MEDIUM" : "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "PETROVA ETL transformations"
}
