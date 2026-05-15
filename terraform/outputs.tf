output "data_lake_name" {
  value = azurerm_storage_account.data_lake.name
}

output "adf_name" {
  value = azurerm_data_factory.petrova.name
}

output "snowflake_database" {
  value = snowflake_database.prod.name
}
