import os
import sys
from pathlib import Path
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_postgres import PGVector
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from tests.test_yaml_txet import yaml_to_structured_sections  # type: ignore[import]

load_dotenv()

# 1. Configure your Embeddings Model
model_name = "jinaai/jina-embeddings-v3"
embeddings = HuggingFaceEmbeddings(model_name=model_name, model_kwargs={"trust_remote_code": True})

# 2. Configure your Vector Database Connection
CONNECTION_STRING = os.getenv("POSTGRES_CONNECTION_STRING")
COLLECTION_NAME = "boxmaster_docs"

vector_store = PGVector(
    embeddings=embeddings,
    collection_name=COLLECTION_NAME,
    connection=CONNECTION_STRING,
    use_jsonb=True,
)

# 3. Load YAML definition and structure
file_path = r"config\schemas\avamed_db\dbo\Dispense.yaml"
schema_payload = yaml_to_structured_sections(file_path)
minimal_summary = (schema_payload["minimal_summary"] or "").strip()
sections = schema_payload["sections"]

text_splitter = RecursiveCharacterTextSplitter(chunk_size=2000, chunk_overlap=100)
base_metadata = {
    "schema": schema_payload["schema"],
    "table": schema_payload["table_name"],
}

chunk_definitions = []

if minimal_summary:
    chunk_definitions.append(
        {
            "content": minimal_summary,
            "metadata": {
                **base_metadata,
                "section": "summary",
                "chunk_type": "table_summary",
            },
        }
    )
    print(
        f"[embedding] summary chunk length={len(minimal_summary)} chunk_size={text_splitter._chunk_size}"
    )

for section in sections:
    section_text = (section.get("text") or "").strip()
    if not section_text:
        continue
    chunk_source = (
        f"SECTION: {section['name'].upper()}\n{section_text}"
    )
    for chunk_index, chunk in enumerate(text_splitter.split_text(chunk_source), start=1):
        chunk_length = len(chunk)
        print(
            f"[embedding] section={section['name']} chunk_index={chunk_index} chunk_length={chunk_length} chunk_size={text_splitter._chunk_size}"
        )
        chunk_definitions.append(
            {
                "content": chunk,
                "metadata": {
                    **base_metadata,
                    "section": section["name"],
                    "chunk_type": "section",
                    "chunk_index": chunk_index,
                },
            }
        )

documents = [
    Document(page_content=chunk_def["content"], metadata=chunk_def["metadata"])
    for chunk_def in chunk_definitions
]

# 4. Generate Embeddings and Store them in the Database
vector_store.add_documents(documents)
print("Documents and embeddings successfully added to the database!")
