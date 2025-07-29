# DataLumos Knowledge Management

The DataLumos knowledge management system provides a vector store for uploading, managing, and searching documents. This system uses OpenAI's File Search functionality to provide relevant context to agents automatically.

## What is the Vector Store?

The vector store is a knowledge base that stores documents and makes them searchable. When documents are added to the DataLumos vector store, they are automatically included in the context of DataLumos agents, enhancing their ability to provide relevant and accurate responses based on your specific documentation.

Key features:
- **Automatic Context Integration**: Documents added to the vector store are automatically available to all DataLumos agents
- **Multiple File Formats**: Supports PDF, TXT, MD, DOC, DOCX, HTML, JSON, CSV, and TSV files
- **Web and Filesystem Sources**: Upload files from your local filesystem or directly from web URLs
- **OpenAI Integration**: Uses OpenAI's File Search API for efficient document retrieval

## CLI Commands

### Adding Documents

Add a document from your filesystem:
```bash
datalumos-knowledge add /path/to/document.pdf
```

Add a document from a web URL:
```bash
datalumos-knowledge add https://example.com/document.pdf --type web
```

### Listing Documents

View all documents in the vector store:
```bash
datalumos-knowledge list
```

This displays a table showing:
- File ID
- Status (processing/completed/failed)
- Creation timestamp

### Deleting Documents

Delete a specific document by its file ID:
```bash
datalumos-knowledge delete <file-id>
```

Skip confirmation prompt:
```bash
datalumos-knowledge delete <file-id> --yes
```

## Python API

### Basic Usage

```python
from datalumos.knowledge.manager import KnowledgeManager

# Initialize the manager
manager = KnowledgeManager()

# Get or create the default DataLumos vector store
store_id = manager.get_or_create_default_store()

# Add a document from filesystem
file_id = manager.upload_and_add_document(
    vector_store_id=store_id,
    source="/path/to/document.pdf",
    source_type="filesystem"
)

# Add a document from web
file_id = manager.upload_and_add_document(
    vector_store_id=store_id,
    source="https://example.com/doc.pdf",
    source_type="web"
)
```

### Advanced Operations

#### Create Custom Vector Stores

```python
# Create a new vector store
custom_store_id = manager.create_vector_store(
    name="my-custom-store",
    metadata={"project": "special-project"}
)

# Create a complete knowledge base with multiple documents
documents = [
    {"source": "/path/to/doc1.pdf", "type": "filesystem"},
    {"source": "https://example.com/doc2.pdf", "type": "web"}
]

kb_id = manager.create_knowledge_base(
    name="project-knowledge-base",
    documents=documents
)
```

#### List and Manage Vector Stores

```python
# List all vector stores
stores = manager.list_vector_stores()
for store in stores:
    print(f"Store: {store['name']} (ID: {store['id']})")
    print(f"Files: {store['file_counts']}")

# Get files in a specific vector store
files = manager.get_vector_store_files(store_id)
for file in files:
    print(f"File ID: {file['id']}, Status: {file['status']}")

# Delete a vector store
manager.delete_vector_store(store_id)

# Delete a specific file
manager.delete_file(file_id)
```

## Supported File Types

The following file formats are supported:
- **PDF**: `.pdf`
- **Text**: `.txt`, `.md`
- **Microsoft Word**: `.doc`, `.docx`
- **Web**: `.html`
- **Data**: `.json`, `.csv`, `.tsv`

## Environment Setup

Make sure you have your OpenAI API key configured:
```bash
export OPENAI_API_KEY="your-api-key-here"
```

Or provide it directly when initializing the manager:
```python
manager = KnowledgeManager(openai_api_key="your-api-key")
```

## Integration with DataLumos Agents

When documents are added to the DataLumos vector store (the default store named "datalumos"), they become automatically available to all DataLumos agents. The agents can search through this knowledge base to provide more accurate and contextual responses based on your specific documentation.

The system automatically:
1. Detects when the DataLumos vector store exists and contains documents
2. Enables file search capabilities for all agents
3. Provides relevant document excerpts as context during agent conversations

This seamless integration ensures that your agents have access to your organization's knowledge without requiring manual configuration for each interaction.