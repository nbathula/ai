###############################################################################
# Sprint 2 — AWS MSK Cluster for Sales Companion
# Kafka broker for Salesforce CDC events → Delta Live Tables
###############################################################################

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region"        { default = "us-east-1" }
variable "environment"       { default = "prod" }
variable "vpc_id"            { description = "VPC where MSK and Databricks both live" }
variable "private_subnet_ids" {
  type        = list(string)
  description = "At least 2 private subnets (multi-AZ)"
}
variable "databricks_security_group_id" {
  description = "SG attached to Databricks clusters — needs MSK access"
}


# ── Security group ─────────────────────────────────────────────────────────────

resource "aws_security_group" "msk" {
  name        = "sales-companion-msk"
  description = "MSK broker access for Sales Companion"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Kafka plaintext (Databricks)"
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
    security_groups = [var.databricks_security_group_id]
  }

  ingress {
    description     = "Kafka TLS (Databricks)"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [var.databricks_security_group_id]
  }

  ingress {
    description     = "Kafka Connect (same SG)"
    from_port       = 9092
    to_port         = 9094
    protocol        = "tcp"
    self            = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "sales-companion-msk"
    Environment = var.environment
    Project     = "sales-companion"
  }
}


# ── MSK Cluster ────────────────────────────────────────────────────────────────

resource "aws_msk_cluster" "sales_companion" {
  cluster_name           = "sales-companion-${var.environment}"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3   # 1 per AZ for HA

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 100   # GB per broker
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.sales_companion.arn
    revision = aws_msk_configuration.sales_companion.latest_revision
  }

  enhanced_monitoring = "PER_TOPIC_PER_PARTITION"

  open_monitoring {
    prometheus {
      jmx_exporter  { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = {
    Environment = var.environment
    Project     = "sales-companion"
    ManagedBy   = "terraform"
  }
}


# ── MSK Broker configuration ───────────────────────────────────────────────────

resource "aws_msk_configuration" "sales_companion" {
  name              = "sales-companion-${var.environment}"
  kafka_versions    = ["3.5.1"]
  server_properties = <<-EOT
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=6
    log.retention.hours=168
    log.retention.bytes=10737418240
    compression.type=lz4
  EOT
}


# ── CloudWatch log group ───────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/sales-companion-${var.environment}"
  retention_in_days = 30
}


# ── Kafka Connect cluster (MSK Connect) ───────────────────────────────────────

resource "aws_mskconnect_connector" "salesforce_cdc" {
  name = "salesforce-cdc-${var.environment}"

  kafkaconnect_version = "2.7.1"

  capacity {
    autoscaling {
      mcu_count        = 1
      min_worker_count = 1
      max_worker_count = 4
      scale_in_policy  { cpu_utilization_percentage = 20 }
      scale_out_policy { cpu_utilization_percentage = 80 }
    }
  }

  connector_configuration = {
    "connector.class"                        = "com.salesforce.kafka.connect.SalesforceSourceConnector"
    "tasks.max"                              = "2"
    "salesforce.instance"                    = var.salesforce_instance_url
    "salesforce.username"                    = var.salesforce_username
    "salesforce.password"                    = var.salesforce_password
    "salesforce.password.token"              = var.salesforce_security_token
    "salesforce.consumer.key"                = var.salesforce_consumer_key
    "salesforce.consumer.secret"             = var.salesforce_consumer_secret
    "salesforce.push.topic.name"             = "OpportunityChanges"
    "kafka.topic"                            = "salesforce.opportunity.cdc"
    "value.converter"                        = "org.apache.kafka.connect.json.JsonConverter"
    "value.converter.schemas.enable"         = "false"
    "key.converter"                          = "org.apache.kafka.connect.storage.StringConverter"
  }

  kafka_cluster {
    apache_kafka_cluster {
      bootstrap_servers = aws_msk_cluster.sales_companion.bootstrap_brokers_tls
      vpc {
        security_groups = [aws_security_group.msk.id]
        subnets         = var.private_subnet_ids
      }
    }
  }

  kafka_cluster_client_authentication {
    authentication_type = "NONE"
  }

  kafka_cluster_encryption_in_transit {
    encryption_type = "TLS"
  }

  plugin {
    custom_plugin {
      arn      = aws_mskconnect_custom_plugin.salesforce.arn
      revision = aws_mskconnect_custom_plugin.salesforce.latest_revision
    }
  }

  service_execution_role_arn = aws_iam_role.msk_connect.arn
}


# ── Salesforce connector plugin (uploaded to S3) ───────────────────────────────

resource "aws_mskconnect_custom_plugin" "salesforce" {
  name         = "salesforce-cdc-plugin-${var.environment}"
  content_type = "ZIP"

  location {
    s3 {
      bucket_arn = aws_s3_bucket.msk_plugins.arn
      file_key   = "plugins/salesforce-kafka-connector.zip"
    }
  }
}

resource "aws_s3_bucket" "msk_plugins" {
  bucket = "sales-companion-msk-plugins-${var.environment}"
}

resource "aws_s3_bucket_versioning" "msk_plugins" {
  bucket = aws_s3_bucket.msk_plugins.id
  versioning_configuration { status = "Enabled" }
}


# ── IAM for MSK Connect ────────────────────────────────────────────────────────

resource "aws_iam_role" "msk_connect" {
  name = "sales-companion-msk-connect-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kafkaconnect.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "msk_connect" {
  name = "sales-companion-msk-connect-policy"
  role = aws_iam_role.msk_connect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.msk_plugins.arn,
          "${aws_s3_bucket.msk_plugins.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = "*"
      }
    ]
  })
}


# ── Variables for Salesforce credentials (set via tfvars or AWS Secrets) ──────

variable "salesforce_instance_url"  { sensitive = true }
variable "salesforce_username"       { sensitive = true }
variable "salesforce_password"       { sensitive = true }
variable "salesforce_security_token" { sensitive = true }
variable "salesforce_consumer_key"   { sensitive = true }
variable "salesforce_consumer_secret"{ sensitive = true }


# ── Outputs ───────────────────────────────────────────────────────────────────

output "msk_bootstrap_brokers_tls" {
  description = "TLS bootstrap brokers — paste into Databricks MSK connection"
  value       = aws_msk_cluster.sales_companion.bootstrap_brokers_tls
  sensitive   = false
}

output "msk_cluster_arn" {
  value = aws_msk_cluster.sales_companion.arn
}
