---
dataset_info:
  features:
  - name: doc_id
    dtype: string
  - name: title
    dtype: string
  - name: full_title
    dtype: string
  - name: number
    dtype: string
  - name: date
    dtype: string
  - name: nature
    dtype: string
  - name: status
    dtype: string
  - name: nature_delib
    dtype: string
  - name: reconstructed_text
    dtype: string
  - name: chunk_count
    dtype: float64
  - name: hash_id
    dtype: string
  - name: cid
    dtype: string
  - name: etat_juridique
    dtype: string
  - name: text_content
    dtype: string
  splits:
  - name: train
    num_bytes: 1164746
    num_examples: 105
  download_size: 455365
  dataset_size: 1164746
configs:
- config_name: default
  data_files:
  - split: train
    path: data/train-*
license: apache-2.0
task_categories:
- question-answering
language:
- fr
tags:
- legal
size_categories:
- 10K<n<100K
---
---
# 🧾 CNIL Full Documents — Aggregated Dataset

This dataset, **`hulk10/cnil-full-documents`**, is an extended and enriched aggregation of the [**AgentPublic/cnil**](https://huggingface.co/datasets/AgentPublic/cnil) dataset.  
It contains the **complete reconstructed texts** of CNIL (Commission Nationale de l’Informatique et des Libertés) deliberations, opinions, and decisions, combined with structured metadata for advanced analysis and NLP applications.

---

## 📘 Description

The **AgentPublic/cnil** dataset provides structured metadata and reconstructed excerpts from CNIL documents.  
**This version (`cnil-full-documents`) aggregates and enhances** that data by:

- Combining fragmented document chunks into **full, continuous texts**  
- Preserving and normalizing document-level metadata  
- Ensuring a one-to-one mapping between `doc_id` and complete reconstructed text  
- Adding unique document hashes (`hash_id`) for deduplication and consistency checks  

This dataset is designed to enable **large-scale semantic analysis**, **document retrieval**, and **LLM training** on official French data protection texts.

---

## 🧩 Data Structure

| Field | Type | Description |
|-------|------|-------------|
| `cid` | string | Unique identifier for the document. |
| `title` | string | Short title of the document. |
| `full_title` | string | Full official title. |
| `number` | string | Decision or deliberation number. |
| `date` | string | Date of adoption or publication. |
| `nature` | string | Document type (opinion, deliberation, decision, etc.). |
| `status` | string | Administrative or publication status. |
| `nature_delib` | string | Detailed legal nature (e.g., “Deliberation of the College”). |
| `reconstructed_text` | string | Fully reassembled and cleaned text from CNIL sources. |
| `chunk_count` | int64 | Number of chunks aggregated to form the full text. |
| `hash_id` | string | Unique hash of the full document. |

---

## 📊 Dataset Statistics

- **Total examples:** 9,038  
- **Total size:** ~75 MB  
- **Language:** French 🇫🇷  
- **License:** [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)  
- **Original source:** [AgentPublic/cnil](https://huggingface.co/datasets/AgentPublic/cnil)

---

## 🧠 Intended Use Cases

- Training or fine-tuning **French legal NLP** or **LLM** models  
- **Semantic search** and **retrieval-augmented generation (RAG)** on regulatory texts  
- Building **question–answer** or **document understanding** datasets  
- Performing **legal or GDPR-related analytics** on CNIL documents  

---

## ⚙️ Example Usage

```python
from datasets import load_dataset

ds = load_dataset("hulk10/cnil-full-documents")
print(ds["train"][0])
```