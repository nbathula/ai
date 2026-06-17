"""
Sprint 1 — Step 3: Document Ingestion Pipeline
Volumes → parse → chunk → embed → Delta (documents.chunks) → Vector Search sync

Triggered automatically by Databricks Auto Loader when files land in any
sales.documents.* Volume. Also runnable as a scheduled full re-index.
"""

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import fitz  # PyMuPDF
import tiktoken
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
from docx import Document as DocxDocument
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    ArrayType, FloatType, IntegerType, StringType, StructField, StructType,
    TimestampType,
)

spark = SparkSession.builder.getOrCreate()
dbutils = None
try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except ImportError:
    pass

CATALOG = "sales"
SCHEMA = "documents"
CHUNK_TABLE = f"{CATALOG}.{SCHEMA}.chunks"
META_TABLE = f"{CATALOG}.{SCHEMA}.metadata"

VECTOR_SEARCH_ENDPOINT = os.environ.get("VS_ENDPOINT", "sales-companion-vs")
VECTOR_INDEX = f"{CATALOG}.{SCHEMA}.chunks_index"
EMBEDDING_ENDPOINT = os.environ.get("EMBED_ENDPOINT", "bge-large-en")

CHUNK_SIZE_TOKENS = 512
CHUNK_OVERLAP_TOKENS = 50

tokenizer = tiktoken.get_encoding("cl100k_base")

# ── Schema for the chunks table ────────────────────────────────────────────────

CHUNK_SCHEMA = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("doc_id", StringType()),
    StructField("doc_type", StringType()),
    StructField("account_id", StringType()),
    StructField("account_name", StringType()),
    StructField("opportunity_id", StringType()),
    StructField("doc_date", StringType()),
    StructField("content", StringType()),
    StructField("source_page", IntegerType()),
    StructField("chunk_index", IntegerType()),
    StructField("token_count", IntegerType()),
    StructField("embedding", ArrayType(FloatType())),
    StructField("ingested_at", TimestampType()),
])


# ── Parsing ────────────────────────────────────────────────────────────────────

def parse_pdf(path: str) -> list[dict]:
    pages = []
    with fitz.open(path) as doc:
        for page_num, page in enumerate(doc, start=1):
            text = page.get_text("text").strip()
            if text:
                pages.append({"page": page_num, "text": text})
    return pages


def parse_docx(path: str) -> list[dict]:
    doc = DocxDocument(path)
    text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
    return [{"page": 1, "text": text}]


def parse_txt(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return [{"page": 1, "text": f.read()}]


def parse_file(path: str) -> list[dict]:
    ext = Path(path).suffix.lower()
    if ext == ".pdf":
        return parse_pdf(path)
    elif ext in (".docx", ".doc"):
        return parse_docx(path)
    elif ext in (".txt", ".md"):
        return parse_txt(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


# ── Chunking ───────────────────────────────────────────────────────────────────

def chunk_text(text: str, page_num: int) -> Iterator[dict]:
    """Sliding window chunker with token count tracking."""
    tokens = tokenizer.encode(text)
    start = 0
    chunk_idx = 0
    while start < len(tokens):
        end = min(start + CHUNK_SIZE_TOKENS, len(tokens))
        chunk_tokens = tokens[start:end]
        chunk_text = tokenizer.decode(chunk_tokens)
        yield {
            "text": chunk_text.strip(),
            "page": page_num,
            "chunk_index": chunk_idx,
            "token_count": len(chunk_tokens),
        }
        start += CHUNK_SIZE_TOKENS - CHUNK_OVERLAP_TOKENS
        chunk_idx += 1


def extract_chunks(pages: list[dict]) -> list[dict]:
    chunks = []
    for page in pages:
        for chunk in chunk_text(page["text"], page["page"]):
            if chunk["token_count"] >= 20:  # skip near-empty chunks
                chunks.append(chunk)
    return chunks


# ── Embedding ─────────────────────────────────────────────────────────────────

def embed_batch(texts: list[str]) -> list[list[float]]:
    """Call Databricks Model Serving BGE endpoint."""
    import requests
    token = dbutils.secrets.get("sales-companion", "databricks-token") if dbutils else os.environ["DATABRICKS_TOKEN"]
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")

    resp = requests.post(
        f"{host}/serving-endpoints/{EMBEDDING_ENDPOINT}/invocations",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"input": texts},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return [item["embedding"] for item in data["data"]]


def embed_chunks(chunks: list[dict], batch_size: int = 64) -> list[dict]:
    texts = [c["text"] for c in chunks]
    embeddings = []
    for i in range(0, len(texts), batch_size):
        embeddings.extend(embed_batch(texts[i:i + batch_size]))
    for chunk, emb in zip(chunks, embeddings):
        chunk["embedding"] = emb
    return chunks


# ── Metadata extraction ────────────────────────────────────────────────────────

def extract_metadata_from_path(file_path: str) -> dict:
    """
    Infer doc_type, account_id, opportunity_id from the Volume path convention:
      /Volumes/sales/documents/{doc_type}/{account_id}/{file_name}
    """
    parts = Path(file_path).parts
    doc_type = "unknown"
    account_id = None
    opportunity_id = None

    try:
        # /Volumes/catalog/schema/volume_name/...
        vol_idx = next(i for i, p in enumerate(parts) if p == "Volumes")
        doc_type = parts[vol_idx + 3] if len(parts) > vol_idx + 3 else "unknown"
        account_id = parts[vol_idx + 4] if len(parts) > vol_idx + 4 else None
    except (StopIteration, IndexError):
        pass

    # Opportunity ID often in filename: OPP-0012345_contract.pdf
    opp_match = re.search(r"(OPP[-_]\w+)", Path(file_path).name, re.IGNORECASE)
    if opp_match:
        opportunity_id = opp_match.group(1)

    return {
        "doc_type": doc_type,
        "account_id": account_id,
        "opportunity_id": opportunity_id,
    }


def file_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


# ── Skip already-ingested files ────────────────────────────────────────────────

def already_ingested(doc_id: str) -> bool:
    result = spark.sql(f"SELECT 1 FROM {META_TABLE} WHERE doc_id = '{doc_id}' LIMIT 1")
    return result.count() > 0


# ── Write to Delta ─────────────────────────────────────────────────────────────

def write_chunks(chunk_rows: list[dict]):
    df = spark.createDataFrame(chunk_rows, schema=CHUNK_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(CHUNK_TABLE)


def write_metadata(meta: dict):
    row = [(
        meta["doc_id"], meta["file_name"], meta["file_path"],
        meta["doc_type"], meta.get("account_id"), meta.get("account_name"),
        meta.get("opportunity_id"), meta.get("owner_id"),
        meta.get("doc_date"), meta.get("expiry_date"),
        meta.get("source", "volume"), datetime.now(timezone.utc), meta["file_hash"],
    )]
    columns = [
        "doc_id", "file_name", "file_path", "doc_type", "account_id",
        "account_name", "opportunity_id", "owner_id", "doc_date",
        "expiry_date", "source", "ingested_at", "file_hash",
    ]
    df = spark.createDataFrame(row, columns)
    df.write.format("delta").mode("append").saveAsTable(META_TABLE)


# ── Vector Search sync ────────────────────────────────────────────────────────

def trigger_vector_search_sync():
    vsc = VectorSearchClient()
    index = vsc.get_index(
        endpoint_name=VECTOR_SEARCH_ENDPOINT,
        index_name=VECTOR_INDEX,
    )
    index.sync()
    print(f"  ✓ Vector Search sync triggered: {VECTOR_INDEX}")


# ── Main ingestion entry point ────────────────────────────────────────────────

def ingest_file(file_path: str, source: str = "volume") -> dict:
    file_name = Path(file_path).name
    fhash = file_hash(file_path)
    doc_id = str(uuid.uuid5(uuid.NAMESPACE_URL, fhash))

    if already_ingested(doc_id):
        return {"status": "skipped", "doc_id": doc_id, "file": file_name}

    meta = extract_metadata_from_path(file_path)
    meta.update({
        "doc_id": doc_id,
        "file_name": file_name,
        "file_path": file_path,
        "file_hash": fhash,
        "source": source,
    })

    pages = parse_file(file_path)
    raw_chunks = extract_chunks(pages)

    if not raw_chunks:
        return {"status": "empty", "doc_id": doc_id, "file": file_name}

    embedded = embed_chunks(raw_chunks)
    now = datetime.now(timezone.utc)

    chunk_rows = [
        {
            "chunk_id": str(uuid.uuid4()),
            "doc_id": doc_id,
            "doc_type": meta["doc_type"],
            "account_id": meta.get("account_id"),
            "account_name": meta.get("account_name"),
            "opportunity_id": meta.get("opportunity_id"),
            "doc_date": meta.get("doc_date"),
            "content": c["text"],
            "source_page": c["page"],
            "chunk_index": c["chunk_index"],
            "token_count": c["token_count"],
            "embedding": c["embedding"],
            "ingested_at": now,
        }
        for c in embedded
    ]

    write_chunks(chunk_rows)
    write_metadata(meta)

    return {
        "status": "ingested",
        "doc_id": doc_id,
        "file": file_name,
        "chunks": len(chunk_rows),
    }


def run_batch(volume_paths: list[str]):
    """Ingest all files from the given Volume paths."""
    results = {"ingested": 0, "skipped": 0, "empty": 0, "errors": 0}
    for path in volume_paths:
        try:
            result = ingest_file(path)
            results[result["status"]] += 1
            if result["status"] == "ingested":
                print(f"  ✓ {result['file']} → {result['chunks']} chunks")
            else:
                print(f"  · {result['file']} ({result['status']})")
        except Exception as e:
            results["errors"] += 1
            print(f"  ✗ {path}: {e}")

    trigger_vector_search_sync()

    print(f"\n✅ Batch complete — {results}")
    return results


# ── Auto Loader entry point (called by Databricks Workflow) ───────────────────

def run_autoloader():
    """
    Stream new files from all document Volumes using Auto Loader.
    Run as a Databricks Streaming Workflow task.
    """
    def process_batch(batch_df, batch_id):
        paths = [row.path for row in batch_df.select("path").collect()]
        if paths:
            run_batch(paths)

    (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("pathGlobFilter", "*.{pdf,docx,doc,txt,md}")
        .load(f"/Volumes/{CATALOG}/{SCHEMA}/")
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"/Volumes/{CATALOG}/{SCHEMA}/_checkpoints/ingestion")
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    run_autoloader()
