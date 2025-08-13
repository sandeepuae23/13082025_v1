# Oracle to Elasticsearch Migration: Deep Dive

## Table of Contents
1. [Migration Architecture Overview](#migration-architecture-overview)
2. [Data Type Mapping Strategies](#data-type-mapping-strategies)
3. [Performance Optimization Techniques](#performance-optimization-techniques)
4. [Complex Query Translation](#complex-query-translation)
5. [Real-time Data Synchronization](#real-time-data-synchronization)
6. [Migration Monitoring and Logging](#migration-monitoring-and-logging)
7. [Error Handling and Recovery](#error-handling-and-recovery)
8. [Best Practices and Gotchas](#best-practices-and-gotchas)

## Migration Architecture Overview

### Core Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Oracle DB     │───▶│ Migration Tool  │───▶│ Elasticsearch   │
│                 │    │                 │    │                 │
│ • Tables        │    │ • Data Extract  │    │ • Indices       │
│ • Views         │    │ • Transform     │    │ • Documents     │
│ • Procedures    │    │ • Load (ETL)    │    │ • Mappings      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Migration Strategies

#### 1. Full Migration
- **Use Case**: One-time complete data transfer
- **Approach**: Extract all data, transform, and bulk load
- **Best For**: Historical data, data warehousing scenarios

#### 2. Incremental Migration
- **Use Case**: Ongoing synchronization
- **Approach**: Track changes using timestamps or change logs
- **Best For**: Live systems requiring near real-time sync

#### 3. Hybrid Migration
- **Use Case**: Initial bulk load + ongoing incremental updates
- **Approach**: Full migration followed by CDC (Change Data Capture)
- **Best For**: Production systems with minimal downtime requirements

## Data Type Mapping Strategies

### Oracle to Elasticsearch Type Mappings

| Oracle Data Type | Elasticsearch Type | Mapping Strategy | Notes |
|------------------|-------------------|------------------|-------|
| `VARCHAR2` | `text` or `keyword` | Content-based decision | Use `text` for searchable content, `keyword` for exact matches |
| `NUMBER` | `long`, `double`, `scaled_float` | Precision-based | Check decimal places and range |
| `DATE` | `date` | Format conversion | Convert to ISO 8601 format |
| `TIMESTAMP` | `date` | Timezone handling | Normalize to UTC |
| `CLOB` | `text` | Full-text indexing | Consider analyzer settings |
| `BLOB` | `binary` or external storage | Size consideration | Large files should use external storage |
| `RAW` | `binary` | Direct mapping | Base64 encoding |

### Advanced Type Handling

#### Nested Object Mapping
```json
{
  "mappings": {
    "properties": {
      "customer": {
        "type": "object",
        "properties": {
          "id": {"type": "long"},
          "name": {"type": "text"},
          "addresses": {
            "type": "nested",
            "properties": {
              "type": {"type": "keyword"},
              "street": {"type": "text"},
              "city": {"type": "keyword"}
            }
          }
        }
      }
    }
  }
}
```

#### Dynamic Templates
```json
{
  "dynamic_templates": [
    {
      "dates": {
        "match": "*_date",
        "mapping": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
        }
      }
    },
    {
      "strings": {
        "match_mapping_type": "string",
        "mapping": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        }
      }
    }
  ]
}
```

## Performance Optimization Techniques

### Batch Processing Strategies

#### 1. Bulk API Optimization
```python
def bulk_index_documents(es_client, documents, index_name, batch_size=1000):
    """
    Optimized bulk indexing with error handling and retry logic
    """
    actions = []
    for doc in documents:
        action = {
            "_index": index_name,
            "_source": doc
        }
        actions.append(action)
        
        if len(actions) >= batch_size:
            success, failed = bulk(
                es_client, 
                actions, 
                request_timeout=60,
                max_retries=3,
                initial_backoff=2,
                max_backoff=600
            )
            actions = []
            yield success, failed
    
    # Process remaining documents
    if actions:
        success, failed = bulk(es_client, actions)
        yield success, failed
```

#### 2. Parallel Processing
```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class ParallelMigration:
    def __init__(self, oracle_conn, es_client, num_workers=4):
        self.oracle_conn = oracle_conn
        self.es_client = es_client
        self.num_workers = num_workers
        self.progress_lock = threading.Lock()
        
    def migrate_table_partitions(self, table_name, partition_column, num_partitions):
        """
        Migrate table data using parallel partition processing
        """
        partitions = self._create_partitions(table_name, partition_column, num_partitions)
        
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            future_to_partition = {
                executor.submit(self._migrate_partition, partition): partition 
                for partition in partitions
            }
            
            for future in as_completed(future_to_partition):
                partition = future_to_partition[future]
                try:
                    result = future.result()
                    self._update_progress(partition, result)
                except Exception as e:
                    logger.error(f"Partition {partition} failed: {e}")
```

### Memory Management

#### Streaming Data Processing
```python
def stream_oracle_data(cursor, query, fetch_size=10000):
    """
    Stream data from Oracle to avoid memory issues with large datasets
    """
    cursor.arraysize = fetch_size
    cursor.execute(query)
    
    while True:
        rows = cursor.fetchmany(fetch_size)
        if not rows:
            break
        yield rows
```

## Complex Query Translation

### JOIN Operations to Nested Documents

#### Oracle Query
```sql
SELECT 
    o.order_id,
    o.order_date,
    o.total_amount,
    c.customer_name,
    c.email,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    p.product_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_date >= '2024-01-01'
```

#### Elasticsearch Document Structure
```json
{
  "order_id": 12345,
  "order_date": "2024-01-15T10:30:00Z",
  "total_amount": 299.99,
  "customer": {
    "name": "John Doe",
    "email": "john.doe@example.com"
  },
  "items": [
    {
      "product_id": 101,
      "product_name": "Widget A",
      "quantity": 2,
      "unit_price": 49.99
    },
    {
      "product_id": 102,
      "product_name": "Widget B",
      "quantity": 1,
      "unit_price": 199.99
    }
  ]
}
```

### Aggregation Translation

#### Oracle Aggregation
```sql
SELECT 
    EXTRACT(YEAR FROM order_date) as year,
    EXTRACT(MONTH FROM order_date) as month,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM orders 
GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
ORDER BY year, month
```

#### Elasticsearch Aggregation
```json
{
  "aggs": {
    "orders_by_date": {
      "date_histogram": {
        "field": "order_date",
        "calendar_interval": "month",
        "format": "yyyy-MM"
      },
      "aggs": {
        "order_count": {
          "value_count": {
            "field": "order_id"
          }
        },
        "total_revenue": {
          "sum": {
            "field": "total_amount"
          }
        },
        "avg_order_value": {
          "avg": {
            "field": "total_amount"
          }
        }
      }
    }
  }
}
```

## Real-time Data Synchronization

### Change Data Capture (CDC) Implementation

#### 1. Timestamp-based CDC
```python
class TimestampBasedCDC:
    def __init__(self, oracle_conn, es_client, table_config):
        self.oracle_conn = oracle_conn
        self.es_client = es_client
        self.table_config = table_config
        self.last_sync_time = self._get_last_sync_time()
    
    def sync_changes(self):
        """
        Sync changes based on timestamp columns
        """
        for table in self.table_config:
            timestamp_column = table['timestamp_column']
            
            # Get changed records
            query = f"""
                SELECT * FROM {table['name']} 
                WHERE {timestamp_column} > :last_sync
                ORDER BY {timestamp_column}
            """
            
            cursor = self.oracle_conn.cursor()
            cursor.execute(query, {'last_sync': self.last_sync_time})
            
            batch = []
            for row in cursor:
                doc = self._transform_row(row, table)
                batch.append(doc)
                
                if len(batch) >= 1000:
                    self._bulk_update_elasticsearch(batch, table['es_index'])
                    batch = []
            
            if batch:
                self._bulk_update_elasticsearch(batch, table['es_index'])
            
            self._update_last_sync_time()
```

#### 2. Oracle Streams/LogMiner Integration
```python
class OracleLogMinerCDC:
    def __init__(self, oracle_conn, es_client):
        self.oracle_conn = oracle_conn
        self.es_client = es_client
    
    def start_logminer_session(self, start_scn=None):
        """
        Start LogMiner session for CDC
        """
        cursor = self.oracle_conn.cursor()
        
        if start_scn:
            cursor.execute("""
                BEGIN
                    DBMS_LOGMNR.START_LOGMNR(
                        startScn => :start_scn,
                        options => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                    );
                END;
            """, {'start_scn': start_scn})
        else:
            cursor.execute("""
                BEGIN
                    DBMS_LOGMNR.START_LOGMNR(
                        options => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
                    );
                END;
            """)
    
    def process_redo_logs(self):
        """
        Process redo log entries and sync to Elasticsearch
        """
        cursor = self.oracle_conn.cursor()
        cursor.execute("""
            SELECT 
                SCN,
                TIMESTAMP,
                OPERATION,
                SEG_NAME,
                SQL_REDO,
                SQL_UNDO
            FROM V$LOGMNR_CONTENTS
            WHERE SEG_NAME IN ('ORDERS', 'CUSTOMERS', 'PRODUCTS')
            ORDER BY SCN
        """)
        
        for row in cursor:
            self._process_change_event(row)
```

## Migration Monitoring and Logging

### Comprehensive Monitoring Dashboard

#### Metrics Collection
```python
from dataclasses import dataclass
from datetime import datetime
import threading

@dataclass
class MigrationMetrics:
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    records_per_second: float = 0.0
    start_time: datetime = None
    current_table: str = ""
    errors: list = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class MigrationMonitor:
    def __init__(self):
        self.metrics = MigrationMetrics()
        self.lock = threading.Lock()
        self.start_time = datetime.now()
    
    def update_progress(self, processed_count, failed_count=0, table_name=None):
        with self.lock:
            self.metrics.processed_records += processed_count
            self.metrics.failed_records += failed_count
            
            if table_name:
                self.metrics.current_table = table_name
            
            # Calculate records per second
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            if elapsed_time > 0:
                self.metrics.records_per_second = self.metrics.processed_records / elapsed_time
    
    def add_error(self, error_message, table_name=None, record_id=None):
        with self.lock:
            error_entry = {
                'timestamp': datetime.now().isoformat(),
                'message': error_message,
                'table': table_name,
                'record_id': record_id
            }
            self.metrics.errors.append(error_entry)
    
    def get_progress_percentage(self):
        with self.lock:
            if self.metrics.total_records == 0:
                return 0
            return (self.metrics.processed_records / self.metrics.total_records) * 100
```

### Advanced Logging Configuration
```python
import logging
import logging.handlers
from datetime import datetime

class MigrationLogger:
    def __init__(self, log_level=logging.INFO):
        self.logger = logging.getLogger('migration')
        self.logger.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        
        # File handler with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            f'migration_{datetime.now().strftime("%Y%m%d")}.log',
            maxBytes=50*1024*1024,  # 50MB
            backupCount=5
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
    
    def log_table_start(self, table_name, record_count):
        self.logger.info(f"Starting migration for table '{table_name}' with {record_count} records")
    
    def log_batch_complete(self, table_name, batch_size, total_processed):
        self.logger.info(f"Completed batch of {batch_size} records for '{table_name}'. Total: {total_processed}")
    
    def log_error(self, table_name, error_message, record_data=None):
        error_msg = f"Error in table '{table_name}': {error_message}"
        if record_data:
            error_msg += f" | Record: {record_data}"
        self.logger.error(error_msg)
```

## Error Handling and Recovery

### Resilient Error Handling Strategy

#### 1. Retry Logic with Exponential Backoff
```python
import time
import random
from functools import wraps

def retry_with_backoff(max_retries=3, base_delay=1, max_delay=60, exponential_base=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        raise e
                    
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    jitter = random.uniform(0, 0.1) * delay
                    time.sleep(delay + jitter)
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {delay:.2f}s")
        return wrapper
    return decorator

@retry_with_backoff(max_retries=3, base_delay=2)
def bulk_index_with_retry(es_client, documents, index_name):
    return bulk(es_client, documents, index=index_name)
```

#### 2. Dead Letter Queue Implementation
```python
class DeadLetterQueue:
    def __init__(self, storage_path='failed_records'):
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)
    
    def add_failed_record(self, table_name, record_data, error_message):
        """
        Store failed records for later reprocessing
        """
        timestamp = datetime.now().isoformat()
        failed_record = {
            'timestamp': timestamp,
            'table_name': table_name,
            'record_data': record_data,
            'error_message': error_message
        }
        
        filename = f"{self.storage_path}/{table_name}_{timestamp.replace(':', '-')}.json"
        with open(filename, 'w') as f:
            json.dump(failed_record, f, indent=2)
    
    def reprocess_failed_records(self, table_name=None):
        """
        Reprocess failed records from dead letter queue
        """
        pattern = f"{self.storage_path}/{table_name}_*.json" if table_name else f"{self.storage_path}/*.json"
        failed_files = glob.glob(pattern)
        
        for file_path in failed_files:
            try:
                with open(file_path, 'r') as f:
                    failed_record = json.load(f)
                
                # Attempt to reprocess
                success = self._reprocess_record(failed_record)
                if success:
                    os.remove(file_path)
                    logger.info(f"Successfully reprocessed failed record from {file_path}")
            except Exception as e:
                logger.error(f"Failed to reprocess record from {file_path}: {e}")
```

## Best Practices and Gotchas

### Performance Best Practices

1. **Index Settings Optimization**
```json
{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0,
    "refresh_interval": "30s",
    "index.mapping.total_fields.limit": 2000,
    "index.max_result_window": 50000
  }
}
```

2. **Bulk Request Sizing**
- Optimal bulk size: 5-15MB per request
- Monitor ES queue size and adjust accordingly
- Use `_bulk` API with proper error handling

3. **Memory Management**
- Use streaming for large datasets
- Implement proper connection pooling
- Monitor JVM heap usage on ES cluster

### Common Gotchas and Solutions

#### 1. Date Format Inconsistencies
```python
def normalize_oracle_date(oracle_date):
    """
    Handle various Oracle date formats
    """
    if isinstance(oracle_date, datetime):
        return oracle_date.isoformat()
    elif isinstance(oracle_date, str):
        # Handle common Oracle date string formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%d-%b-%Y %H:%M:%S',
            '%Y-%m-%d'
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(oracle_date, fmt)
                return dt.isoformat()
            except ValueError:
                continue
    return None
```

#### 2. Large Text Field Handling
```python
def handle_large_text_field(text_value, max_length=32766):
    """
    Handle large text fields that exceed ES limits
    """
    if text_value and len(text_value) > max_length:
        # Store full text in separate field or external storage
        return {
            'text_preview': text_value[:max_length],
            'full_text_id': store_in_external_storage(text_value),
            'is_truncated': True
        }
    return text_value
```

#### 3. NULL Value Handling
```python
def clean_document_for_es(doc):
    """
    Clean document by removing null values and handling special cases
    """
    cleaned = {}
    for key, value in doc.items():
        if value is not None:
            if isinstance(value, dict):
                cleaned_nested = clean_document_for_es(value)
                if cleaned_nested:  # Only add if not empty
                    cleaned[key] = cleaned_nested
            elif isinstance(value, list):
                cleaned_list = [clean_document_for_es(item) if isinstance(item, dict) else item 
                              for item in value if item is not None]
                if cleaned_list:
                    cleaned[key] = cleaned_list
            else:
                cleaned[key] = value
    return cleaned
```

### Migration Validation

#### Data Integrity Checks
```python
class MigrationValidator:
    def __init__(self, oracle_conn, es_client):
        self.oracle_conn = oracle_conn
        self.es_client = es_client
    
    def validate_record_counts(self, table_name, es_index):
        """
        Compare record counts between Oracle and Elasticsearch
        """
        # Oracle count
        cursor = self.oracle_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        oracle_count = cursor.fetchone()[0]
        
        # Elasticsearch count
        es_count = self.es_client.count(index=es_index)['count']
        
        return {
            'oracle_count': oracle_count,
            'elasticsearch_count': es_count,
            'match': oracle_count == es_count,
            'difference': oracle_count - es_count
        }
    
    def validate_sample_records(self, table_name, es_index, sample_size=100):
        """
        Validate a sample of records for data accuracy
        """
        # Get random sample from Oracle
        cursor = self.oracle_conn.cursor()
        cursor.execute(f"""
            SELECT * FROM (
                SELECT * FROM {table_name} ORDER BY DBMS_RANDOM.VALUE
            ) WHERE ROWNUM <= {sample_size}
        """)
        
        oracle_records = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        validation_results = []
        for record in oracle_records:
            oracle_doc = dict(zip(column_names, record))
            
            # Find corresponding ES document
            es_query = {
                "query": {
                    "term": {
                        "id": oracle_doc['ID']  # Assuming ID is the primary key
                    }
                }
            }
            
            es_result = self.es_client.search(index=es_index, body=es_query)
            
            if es_result['hits']['total']['value'] > 0:
                es_doc = es_result['hits']['hits'][0]['_source']
                match_result = self._compare_documents(oracle_doc, es_doc)
                validation_results.append(match_result)
            else:
                validation_results.append({
                    'id': oracle_doc['ID'],
                    'found_in_es': False,
                    'matches': False
                })
        
        return validation_results
```

This comprehensive deep dive covers the essential aspects of Oracle to Elasticsearch migration, providing both theoretical knowledge and practical implementation strategies for successful data migration projects.