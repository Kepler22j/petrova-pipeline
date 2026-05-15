# PETROVA – SSIS Package Documentation

## Overview
SSIS handles the **on-premises legacy bridge** in the Triple Orchestration pattern.
It runs on SQL Server Integration Services (on-prem) and is triggered/monitored by Airflow.

## Packages

### 1. `PETROVA_SCD2_Vendor_Load.dtsx`
- **Purpose**: SCD Type 2 load for vendor dimension
- **Pattern**: SSIS Lookup Transform → Snowflake MERGE
- **Flow**:
  1. OLE DB Source → SAP vendor extract
  2. Lookup Transform → compare against current Silver records
  3. Conditional Split → New / Changed / Unchanged
  4. OLE DB Destination → staging table
  5. Execute SQL Task → call `SP_MERGE_VENDORS_SCD2`

### 2. `PETROVA_Bronze_SAP_Extract.dtsx`
- **Purpose**: Extract SAP tables (orders, materials, vendors) to Bronze
- **Flow**:
  1. SAP BODS connection → flat file extract
  2. Data Conversion → standardize types
  3. Snowflake ODBC Destination → Bronze tables
  4. Row count + audit logging

### 3. `PETROVA_Data_Validation.dtsx`
- **Purpose**: Bronze Gate schema validation on-prem
- **C# Script Component**: Custom validation logic
- **Checks**: Required fields, data type validation, referential integrity

## C# Validation Script Reference
```csharp
// Script Component: SC_BronzeGateValidation
// Location: PETROVA_Data_Validation.dtsx > Data Flow Task
public override void Input0_ProcessInputRow(Input0Buffer Row)
{
    // Schema validation
    Row.IsValid = !Row.SensorID_IsNull
                  && !Row.ReadingTimestamp_IsNull
                  && Row.ReadingValue >= -9999
                  && Row.ReadingValue <= 99999;

    Row.QualityFlag = Row.IsValid ? "PASS" : "FAIL";
    Row.ValidatedAt = DateTime.UtcNow;
}
```

## Deployment
- SSIS packages deployed via SSISDB catalog
- Environment variables for connection strings
- SQL Agent Job: `PETROVA_SSIS_Daily` (schedule: 01:00 UTC, before Airflow DAG)
