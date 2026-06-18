"""
Sprint 2 — Kafka Topic Setup
Creates and configures all MSK topics for Sales Companion.
Run once after MSK cluster is provisioned.
"""

import os
from dataclasses import dataclass

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = os.environ["MSK_BOOTSTRAP_SERVERS"].split(",")


@dataclass
class TopicSpec:
    name: str
    partitions: int
    replication: int
    retention_ms: int      # how long to keep messages
    cleanup_policy: str = "delete"


TOPICS = [
    TopicSpec(
        name="salesforce.opportunity.cdc",
        partitions=6,
        replication=3,
        retention_ms=7 * 24 * 60 * 60 * 1000,   # 7 days
    ),
    TopicSpec(
        name="salesforce.account.cdc",
        partitions=6,
        replication=3,
        retention_ms=7 * 24 * 60 * 60 * 1000,
    ),
    TopicSpec(
        name="salesforce.contact.cdc",
        partitions=3,
        replication=3,
        retention_ms=7 * 24 * 60 * 60 * 1000,
    ),
    TopicSpec(
        name="salesforce.activity.cdc",
        partitions=6,
        replication=3,
        retention_ms=3 * 24 * 60 * 60 * 1000,   # activities age out faster
    ),
    # DLQ topics — failed events land here for investigation
    TopicSpec(
        name="sales-companion.dlq.opportunity",
        partitions=1,
        replication=3,
        retention_ms=30 * 24 * 60 * 60 * 1000,  # 30 days for DLQ
    ),
    TopicSpec(
        name="sales-companion.dlq.account",
        partitions=1,
        replication=3,
        retention_ms=30 * 24 * 60 * 60 * 1000,
    ),
]


def create_topics():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="sales-companion-admin",
        security_protocol="SSL",
    )

    new_topics = [
        NewTopic(
            name=t.name,
            num_partitions=t.partitions,
            replication_factor=t.replication,
            topic_configs={
                "retention.ms": str(t.retention_ms),
                "cleanup.policy": t.cleanup_policy,
                "compression.type": "lz4",
                "min.insync.replicas": "2",
            },
        )
        for t in TOPICS
    ]

    for topic in new_topics:
        try:
            admin.create_topics([topic])
            print(f"  ✓ Created: {topic.name}")
        except TopicAlreadyExistsError:
            print(f"  · Exists : {topic.name}")

    admin.close()


def describe_topics():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="sales-companion-describe",
        security_protocol="SSL",
    )
    meta = admin.describe_topics([t.name for t in TOPICS])
    for topic in meta:
        parts = len(topic["partitions"])
        print(f"  {topic['topic']}: {parts} partitions")
    admin.close()


if __name__ == "__main__":
    print("Creating MSK topics...")
    create_topics()
    print("\nTopic state:")
    describe_topics()
    print("\n✅ Topic setup complete")
