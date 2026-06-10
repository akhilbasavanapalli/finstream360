################################################################################
# FinStream360 · Terraform — Azure Infrastructure
################################################################################
# Provisions:
#   • Resource Group
#   • ADLS Gen2 (Data Lake) with Bronze / Silver / Gold / Landing containers
#   • Azure Event Hub (Kafka-compatible real-time ingest)
#   • Azure Databricks Workspace
#   • Azure Data Factory
#   • Key Vault (secrets management)
#   • Log Analytics Workspace (monitoring)
#
# Author : Akhil Basavanapalli
# Tech   : Terraform, Azure, PowerShell
################################################################################

terraform {
  required_version = ">= 1.7.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.47"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-finstream360-tfstate"
    storage_account_name = "stfinstream360tfstate"
    container_name       = "tfstate"
    key                  = "finstream360/terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
  }
}

data "azurerm_client_config" "current" {}

################################################################################
# Resource Group
################################################################################
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.common_tags
}

################################################################################
# ADLS Gen2 — Data Lake
################################################################################
resource "azurerm_storage_account" "datalake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true     # Required for ADLS Gen2
  min_tls_version          = "TLS1_2"

  blob_properties {
    delete_retention_policy { days = 30 }
  }

  tags = local.common_tags
}

# Data Lake containers (zones)
locals {
  datalake_containers = ["landing", "bronze", "silver", "gold", "checkpoints", "mlflow"]
}

resource "azurerm_storage_data_lake_gen2_filesystem" "zones" {
  for_each           = toset(local.datalake_containers)
  name               = each.value
  storage_account_id = azurerm_storage_account.datalake.id
}

################################################################################
# Azure Event Hub (Kafka-compatible real-time streaming)
################################################################################
resource "azurerm_eventhub_namespace" "main" {
  name                = "${var.project_name}-eh-ns-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 2
  kafka_enabled       = true
  auto_inflate_enabled        = true
  maximum_throughput_units    = 10

  tags = local.common_tags
}

resource "azurerm_eventhub" "transactions" {
  name                = "raw-transactions"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 3
}

resource "azurerm_eventhub" "customers" {
  name                = "raw-customers"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 2
  message_retention   = 3
}

resource "azurerm_eventhub_authorization_rule" "producer" {
  name                = "finstream360-producer"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = false
  send                = true
  manage              = false
}

resource "azurerm_eventhub_authorization_rule" "consumer" {
  name                = "finstream360-consumer"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = true
  send                = false
  manage              = false
}

################################################################################
# Azure Databricks Workspace
################################################################################
resource "azurerm_databricks_workspace" "main" {
  name                        = "${var.project_name}-dbx-${var.environment}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium"
  managed_resource_group_name = "${var.project_name}-dbx-managed-${var.environment}"

  custom_parameters {
    no_public_ip        = true
    virtual_network_id  = azurerm_virtual_network.main.id
    private_subnet_name = azurerm_subnet.dbx_private.name
    public_subnet_name  = azurerm_subnet.dbx_public.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.dbx_public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.dbx_private.id
  }

  tags = local.common_tags
}

################################################################################
# Azure Data Factory
################################################################################
resource "azurerm_data_factory" "main" {
  name                = "${var.project_name}-adf-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  identity {
    type = "SystemAssigned"
  }

  github_configuration {
    account_name    = var.github_account
    branch_name     = "main"
    git_url         = "https://github.com"
    repository_name = var.github_repo
    root_folder     = "/ingestion/adf_pipelines"
  }

  tags = local.common_tags
}

################################################################################
# Azure Key Vault
################################################################################
resource "azurerm_key_vault" "main" {
  name                = "${var.project_name}-kv-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"
  soft_delete_retention_days = 7

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "List", "Set", "Delete", "Purge"]
  }

  # ADF managed identity access
  access_policy {
    tenant_id  = data.azurerm_client_config.current.tenant_id
    object_id  = azurerm_data_factory.main.identity[0].principal_id
    secret_permissions = ["Get", "List"]
  }

  tags = local.common_tags
}

################################################################################
# Log Analytics (monitoring)
################################################################################
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.project_name}-law-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.common_tags
}

################################################################################
# Networking (VNet for Databricks private deployment)
################################################################################
resource "azurerm_virtual_network" "main" {
  name                = "${var.project_name}-vnet-${var.environment}"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.common_tags
}

resource "azurerm_subnet" "dbx_public" {
  name                 = "dbx-public"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.1.0/24"]

  delegation {
    name = "databricks"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_subnet" "dbx_private" {
  name                 = "dbx-private"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.2.0/24"]

  delegation {
    name = "databricks"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_network_security_group" "dbx" {
  name                = "${var.project_name}-nsg-dbx-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.common_tags
}

resource "azurerm_subnet_network_security_group_association" "dbx_public" {
  subnet_id                 = azurerm_subnet.dbx_public.id
  network_security_group_id = azurerm_network_security_group.dbx.id
}

resource "azurerm_subnet_network_security_group_association" "dbx_private" {
  subnet_id                 = azurerm_subnet.dbx_private.id
  network_security_group_id = azurerm_network_security_group.dbx.id
}

################################################################################
# Locals
################################################################################
locals {
  common_tags = {
    project     = var.project_name
    environment = var.environment
    owner       = "akhil-basavanapalli"
    managed_by  = "terraform"
  }
}
