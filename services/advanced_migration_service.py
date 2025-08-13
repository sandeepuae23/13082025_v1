"""
Advanced Migration Service for Oracle to Elasticsearch
Implements sophisticated migration strategies, monitoring, and error handling
"""

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Generator, Tuple
import os
import glob

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
import oracledb

from models import MigrationJob, MappingConfiguration, db

logger = logging.getLogger(__name__)

@dataclass
class MigrationMetrics:
    """Comprehensive migration metrics tracking"""
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    records_per_second: float = 0.0
    start_time: datetime = field(default_factory=datetime.now)
    current_table: str = ""
    current_batch: int = 0
    total_batches: int = 0
    errors: List[Dict] = field(default_factory=list)
    memory_usage_mb: float = 0.0
    
    @property
    def progress_percentage(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.processed_records / self.total_records) * 100
    
    @property
    def elapsed_time(self) -> timedelta:
        return datetime.now() - self.start_time
    
    @property
    def estimated_completion(self) -> Optional[datetime]:
        if self.records_per_second == 0 or self.total_records == 0:
            return None
        remaining_records = self.total_records - self.processed_records
        remaining_seconds = remaining_records / self.records_per_second
        return datetime.now() + timedelta(seconds=remaining_seconds)

class DeadLetterQueue:
    """Handles failed records for later reprocessing"""
    
    def __init__(self, storage_path: str = 'failed_records'):
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)
    
    def add_failed_record(self, table_name: str, record_data: Dict, 
                         error_message: str, job_id: int = None):
        """Store failed record for later reprocessing"""
        timestamp = datetime.now().isoformat().replace(':', '-')
        failed_record = {
            'timestamp': timestamp,
            'job_id': job_id,
            'table_name': table_name,
            'record_data': record_data,
            'error_message': error_message,
            'retry_count': 0
        }
        
        filename = f"{self.storage_path}/{table_name}_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(failed_record, f, indent=2, default=str)
        
        logger.warning(f"Added failed record to DLQ: {filename}")
    
    def get_failed_records(self, table_name: str = None) -> List[Dict]:
        """Retrieve failed records for reprocessing"""
        pattern = f"{self.storage_path}/{table_name}_*.json" if table_name else f"{self.storage_path}/*.json"
        failed_files = glob.glob(pattern)
        
        failed_records = []
        for file_path in failed_files:
            try:
                with open(file_path, 'r') as f:
                    record = json.load(f)
                    record['file_path'] = file_path
                    failed_records.append(record)
            except Exception as e:
                logger.error(f"Failed to load failed record from {file_path}: {e}")
        
        return failed_records
    
    def remove_processed_record(self, file_path: str):
        """Remove successfully reprocessed record"""
        try:
            os.remove(file_path)
            logger.info(f"Removed processed record: {file_path}")
        except Exception as e:
            logger.error(f"Failed to remove processed record {file_path}: {e}")

class AdvancedMigrationService:
    """Advanced migration service with comprehensive features"""
    
    def __init__(self, batch_size: int = 5000, max_workers: int = 4):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.metrics = MigrationMetrics()
        self.metrics_lock = threading.Lock()
        self.dlq = DeadLetterQueue()
        self.stop_event = threading.Event()
        
    def start_advanced_migration(self, job_id: int, migration_strategy: str = 'full'):
        """
        Start advanced migration with specified strategy
        
        Args:
            job_id: Migration job ID
            migration_strategy: 'full', 'incremental', or 'hybrid'
        """
        try:
            job = MigrationJob.query.get(job_id)
            if not job:
                raise ValueError(f"Migration job {job_id} not found")
            
            mapping_config = job.mapping_configuration
            
            # Update job status
            job.status = 'running'
            job.start_time = datetime.now()
            db.session.commit()
            
            # Initialize connections
            oracle_conn = self._create_oracle_connection(mapping_config.oracle_connection)
            es_client = self._create_elasticsearch_client(mapping_config.elasticsearch_connection)
            
            # Execute migration based on strategy
            if migration_strategy == 'full':
                self._execute_full_migration(job, oracle_conn, es_client)
            elif migration_strategy == 'incremental':
                self._execute_incremental_migration(job, oracle_conn, es_client)
            elif migration_strategy == 'hybrid':
                self._execute_hybrid_migration(job, oracle_conn, es_client)
            else:
                raise ValueError(f"Unknown migration strategy: {migration_strategy}")
            
            # Update final job status
            job.status = 'completed'
            job.end_time = datetime.now()
            job.processed_records = self.metrics.processed_records
            job.failed_records = self.metrics.failed_records
            
        except Exception as e:
            logger.error(f"Migration job {job_id} failed: {e}")
            job.status = 'failed'
            job.error_message = str(e)
            job.end_time = datetime.now()
        finally:
            db.session.commit()
            if 'oracle_conn' in locals():
                oracle_conn.close()
    
    def _execute_full_migration(self, job: MigrationJob, oracle_conn, es_client):
        """Execute full data migration"""
        mapping_config = job.mapping_configuration
        
        # Get total record count
        cursor = oracle_conn.cursor()
        count_query = f"SELECT COUNT(*) FROM ({mapping_config.oracle_query})"
        cursor.execute(count_query)
        total_count = cursor.fetchone()[0]
        
        with self.metrics_lock:
            self.metrics.total_records = total_count
            self.metrics.current_table = mapping_config.elasticsearch_index
        
        logger.info(f"Starting full migration of {total_count} records")
        
        # Prepare Elasticsearch index
        self._prepare_elasticsearch_index(es_client, mapping_config)
        
        # Stream data and process in batches
        for batch_num, batch_data in enumerate(self._stream_oracle_data(oracle_conn, mapping_config.oracle_query)):
            if self.stop_event.is_set():
                logger.info("Migration stopped by user request")
                break
            
            with self.metrics_lock:
                self.metrics.current_batch = batch_num + 1
            
            # Transform and load batch
            transformed_docs = self._transform_batch(batch_data, mapping_config)
            success_count, failed_count = self._bulk_index_documents(
                es_client, transformed_docs, mapping_config.elasticsearch_index
            )
            
            # Update metrics
            with self.metrics_lock:
                self.metrics.processed_records += success_count
                self.metrics.failed_records += failed_count
                
                # Calculate records per second
                elapsed_seconds = self.metrics.elapsed_time.total_seconds()
                if elapsed_seconds > 0:
                    self.metrics.records_per_second = self.metrics.processed_records / elapsed_seconds
            
            # Update job progress
            job.processed_records = self.metrics.processed_records
            job.failed_records = self.metrics.failed_records
            job.progress_percentage = self.metrics.progress_percentage
            db.session.commit()
            
            logger.info(f"Processed batch {batch_num + 1}: {success_count} success, {failed_count} failed")
    
    def _execute_incremental_migration(self, job: MigrationJob, oracle_conn, es_client):
        """Execute incremental migration based on timestamps"""
        mapping_config = job.mapping_configuration
        
        # Get last sync timestamp from job or configuration
        last_sync = self._get_last_sync_timestamp(job)
        
        # Build incremental query
        incremental_query = self._build_incremental_query(mapping_config.oracle_query, last_sync)
        
        logger.info(f"Starting incremental migration from {last_sync}")
        
        # Process incremental changes
        for batch_data in self._stream_oracle_data(oracle_conn, incremental_query):
            if self.stop_event.is_set():
                break
            
            transformed_docs = self._transform_batch(batch_data, mapping_config)
            success_count, failed_count = self._bulk_index_documents(
                es_client, transformed_docs, mapping_config.elasticsearch_index
            )
            
            with self.metrics_lock:
                self.metrics.processed_records += success_count
                self.metrics.failed_records += failed_count
            
            # Update job progress
            job.processed_records = self.metrics.processed_records
            job.failed_records = self.metrics.failed_records
            db.session.commit()
        
        # Update last sync timestamp
        self._update_last_sync_timestamp(job, datetime.now())
    
    def _execute_hybrid_migration(self, job: MigrationJob, oracle_conn, es_client):
        """Execute hybrid migration (full + incremental)"""
        # First, perform full migration
        self._execute_full_migration(job, oracle_conn, es_client)
        
        # Then set up for incremental updates
        if job.status != 'failed':
            self._update_last_sync_timestamp(job, datetime.now())
            logger.info("Hybrid migration completed. Set up for incremental updates.")
    
    def _stream_oracle_data(self, oracle_conn, query: str, 
                           fetch_size: int = None) -> Generator[List[Dict], None, None]:
        """Stream data from Oracle in batches"""
        if fetch_size is None:
            fetch_size = self.batch_size
        
        cursor = oracle_conn.cursor()
        cursor.arraysize = fetch_size
        cursor.execute(query)
        
        # Get column names
        column_names = [desc[0].lower() for desc in cursor.description]
        
        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break
            
            # Convert rows to dictionaries
            batch = [dict(zip(column_names, row)) for row in rows]
            yield batch
    
    def _transform_batch(self, batch_data: List[Dict], 
                        mapping_config: MappingConfiguration) -> List[Dict]:
        """Transform Oracle data to Elasticsearch documents"""
        transformed_docs = []
        field_mappings = mapping_config.get_field_mappings()
        transformation_rules = mapping_config.get_transformation_rules()
        
        for record in batch_data:
            try:
                # Apply field mappings
                doc = {}
                for oracle_field, es_field in field_mappings.items():
                    if oracle_field in record:
                        value = record[oracle_field]
                        
                        # Apply transformations
                        if es_field in transformation_rules:
                            value = self._apply_transformation(value, transformation_rules[es_field])
                        
                        # Handle data type conversions
                        value = self._convert_data_type(value)
                        
                        if value is not None:
                            doc[es_field] = value
                
                # Add metadata
                doc['_migration_timestamp'] = datetime.now().isoformat()
                doc['_migration_job_id'] = mapping_config.id
                
                transformed_docs.append(doc)
                
            except Exception as e:
                logger.error(f"Failed to transform record: {e}")
                self.dlq.add_failed_record(
                    mapping_config.elasticsearch_index,
                    record,
                    f"Transformation error: {e}",
                    mapping_config.id
                )
        
        return transformed_docs
    
    def _bulk_index_documents(self, es_client, documents: List[Dict], 
                            index_name: str) -> Tuple[int, int]:
        """Bulk index documents with error handling"""
        if not documents:
            return 0, 0
        
        actions = []
        for doc in documents:
            action = {
                '_index': index_name,
                '_source': doc
            }
            # Use document ID if available
            if '_id' in doc:
                action['_id'] = doc['_id']
                del doc['_id']
            
            actions.append(action)
        
        try:
            success_count, failed_items = bulk(
                es_client,
                actions,
                request_timeout=60,
                max_retries=3,
                initial_backoff=2,
                max_backoff=600,
                raise_on_error=False,
                raise_on_exception=False
            )
            
            # Handle failed items
            failed_count = 0
            if failed_items:
                for failed_item in failed_items:
                    failed_count += 1
                    error_info = failed_item.get('index', {}).get('error', 'Unknown error')
                    logger.error(f"Failed to index document: {error_info}")
                    
                    # Add to dead letter queue
                    doc_data = failed_item.get('index', {}).get('_source', {})
                    self.dlq.add_failed_record(
                        index_name,
                        doc_data,
                        f"Indexing error: {error_info}"
                    )
            
            return success_count, failed_count
            
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}")
            # Add all documents to dead letter queue
            for doc in documents:
                self.dlq.add_failed_record(
                    index_name,
                    doc,
                    f"Bulk indexing exception: {e}"
                )
            return 0, len(documents)
    
    def _apply_transformation(self, value: Any, transformation_rule: Dict) -> Any:
        """Apply transformation rules to field values"""
        try:
            rule_type = transformation_rule.get('type')
            
            if rule_type == 'date_format':
                if isinstance(value, str):
                    from_format = transformation_rule.get('from_format', '%Y-%m-%d %H:%M:%S')
                    to_format = transformation_rule.get('to_format', '%Y-%m-%dT%H:%M:%SZ')
                    dt = datetime.strptime(value, from_format)
                    return dt.strftime(to_format)
            
            elif rule_type == 'string_manipulation':
                if isinstance(value, str):
                    operation = transformation_rule.get('operation')
                    if operation == 'uppercase':
                        return value.upper()
                    elif operation == 'lowercase':
                        return value.lower()
                    elif operation == 'trim':
                        return value.strip()
            
            elif rule_type == 'numeric_scaling':
                if isinstance(value, (int, float)):
                    scale_factor = transformation_rule.get('scale_factor', 1)
                    return value * scale_factor
            
            elif rule_type == 'conditional':
                condition = transformation_rule.get('condition')
                if_true = transformation_rule.get('if_true')
                if_false = transformation_rule.get('if_false')
                
                # Simple condition evaluation
                if condition.get('operator') == 'equals':
                    if value == condition.get('value'):
                        return if_true
                    else:
                        return if_false
            
        except Exception as e:
            logger.warning(f"Transformation failed for value {value}: {e}")
        
        return value
    
    def _convert_data_type(self, value: Any) -> Any:
        """Convert Oracle data types to Elasticsearch compatible types"""
        if value is None:
            return None
        
        # Handle Oracle DATE/TIMESTAMP
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        
        # Handle Oracle NUMBER with decimals
        if isinstance(value, (int, float)):
            return value
        
        # Handle Oracle CLOB/BLOB
        if hasattr(value, 'read'):
            try:
                content = value.read()
                if isinstance(content, bytes):
                    # For binary data, consider base64 encoding or external storage
                    import base64
                    return base64.b64encode(content).decode('utf-8')
                return content
            except Exception as e:
                logger.warning(f"Failed to read LOB data: {e}")
                return None
        
        # Convert to string for complex types
        if not isinstance(value, (str, int, float, bool, list, dict)):
            return str(value)
        
        return value
    
    def _prepare_elasticsearch_index(self, es_client, mapping_config: MappingConfiguration):
        """Prepare Elasticsearch index with optimized settings"""
        index_name = mapping_config.elasticsearch_index
        
        # Check if index exists
        if es_client.indices.exists(index=index_name):
            logger.info(f"Index {index_name} already exists")
            return
        
        # Create index with optimized settings for bulk loading
        index_settings = {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 0,  # No replicas during migration
                "refresh_interval": "30s",  # Slower refresh for better performance
                "index.mapping.total_fields.limit": 2000,
                "index.max_result_window": 50000,
                "index.mapping.nested_fields.limit": 100
            },
            "mappings": {
                "properties": {
                    "_migration_timestamp": {"type": "date"},
                    "_migration_job_id": {"type": "integer"}
                }
            }
        }
        
        # Add dynamic templates for common patterns
        index_settings["mappings"]["dynamic_templates"] = [
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
        
        es_client.indices.create(index=index_name, body=index_settings)
        logger.info(f"Created index {index_name} with optimized settings")
    
    def _create_oracle_connection(self, oracle_conn_config):
        """Create Oracle database connection"""
        dsn = oracledb.makedsn(
            oracle_conn_config.host,
            oracle_conn_config.port,
            service_name=oracle_conn_config.service_name
        )
        
        connection = oracledb.connect(
            user=oracle_conn_config.username,
            password=oracle_conn_config.password,
            dsn=dsn,
            encoding="UTF-8"
        )
        
        return connection
    
    def _create_elasticsearch_client(self, es_conn_config):
        """Create Elasticsearch client"""
        hosts = [{'host': es_conn_config.host, 'port': es_conn_config.port}]
        
        es_config = {
            'hosts': hosts,
            'timeout': 60,
            'max_retries': 3,
            'retry_on_timeout': True
        }
        
        if es_conn_config.username and es_conn_config.password:
            es_config['http_auth'] = (es_conn_config.username, es_conn_config.password)
        
        if es_conn_config.use_ssl:
            es_config['use_ssl'] = True
            es_config['verify_certs'] = False  # For development
        
        return Elasticsearch(**es_config)
    
    def _get_last_sync_timestamp(self, job: MigrationJob) -> datetime:
        """Get last synchronization timestamp"""
        # This could be stored in job metadata or separate table
        # For now, return a default timestamp
        return datetime.now() - timedelta(days=1)
    
    def _update_last_sync_timestamp(self, job: MigrationJob, timestamp: datetime):
        """Update last synchronization timestamp"""
        # Store in job metadata or separate table
        logger.info(f"Updated last sync timestamp to {timestamp}")
    
    def _build_incremental_query(self, base_query: str, last_sync: datetime) -> str:
        """Build incremental query with timestamp filter"""
        # This is a simplified approach - in reality, you'd need to analyze
        # the query structure and add appropriate WHERE clauses
        timestamp_str = last_sync.strftime('%Y-%m-%d %H:%M:%S')
        
        if 'WHERE' in base_query.upper():
            return f"{base_query} AND updated_date > TO_DATE('{timestamp_str}', 'YYYY-MM-DD HH24:MI:SS')"
        else:
            return f"{base_query} WHERE updated_date > TO_DATE('{timestamp_str}', 'YYYY-MM-DD HH24:MI:SS')"
    
    def stop_migration(self):
        """Stop the currently running migration"""
        self.stop_event.set()
        logger.info("Migration stop requested")
    
    def get_metrics(self) -> Dict:
        """Get current migration metrics"""
        with self.metrics_lock:
            return {
                'total_records': self.metrics.total_records,
                'processed_records': self.metrics.processed_records,
                'failed_records': self.metrics.failed_records,
                'progress_percentage': self.metrics.progress_percentage,
                'records_per_second': self.metrics.records_per_second,
                'elapsed_time': str(self.metrics.elapsed_time),
                'estimated_completion': self.metrics.estimated_completion.isoformat() if self.metrics.estimated_completion else None,
                'current_table': self.metrics.current_table,
                'current_batch': self.metrics.current_batch,
                'total_batches': self.metrics.total_batches
            }
    
    def reprocess_failed_records(self, table_name: str = None) -> Dict:
        """Reprocess failed records from dead letter queue"""
        failed_records = self.dlq.get_failed_records(table_name)
        
        if not failed_records:
            return {'message': 'No failed records found', 'processed': 0}
        
        processed_count = 0
        for record in failed_records:
            try:
                # Attempt to reprocess the record
                # This would involve re-transforming and re-indexing
                logger.info(f"Reprocessing failed record from {record['file_path']}")
                
                # Mark as processed and remove from DLQ
                self.dlq.remove_processed_record(record['file_path'])
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Failed to reprocess record: {e}")
        
        return {
            'message': f'Reprocessed {processed_count} out of {len(failed_records)} failed records',
            'processed': processed_count,
            'remaining': len(failed_records) - processed_count
        }

# Migration validation service
class MigrationValidator:
    """Validates migration results and data integrity"""
    
    def __init__(self, oracle_conn, es_client):
        self.oracle_conn = oracle_conn
        self.es_client = es_client
    
    def validate_migration(self, oracle_query: str, es_index: str, 
                          sample_size: int = 1000) -> Dict:
        """Comprehensive migration validation"""
        results = {
            'record_count_validation': self._validate_record_counts(oracle_query, es_index),
            'sample_data_validation': self._validate_sample_records(oracle_query, es_index, sample_size),
            'data_type_validation': self._validate_data_types(oracle_query, es_index),
            'index_health': self._check_index_health(es_index)
        }
        
        # Calculate overall validation score
        scores = [
            results['record_count_validation'].get('score', 0),
            results['sample_data_validation'].get('score', 0),
            results['data_type_validation'].get('score', 0),
            results['index_health'].get('score', 0)
        ]
        
        results['overall_score'] = sum(scores) / len(scores)
        results['validation_timestamp'] = datetime.now().isoformat()
        
        return results
    
    def _validate_record_counts(self, oracle_query: str, es_index: str) -> Dict:
        """Validate record counts match between Oracle and Elasticsearch"""
        try:
            # Oracle count
            cursor = self.oracle_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM ({oracle_query})")
            oracle_count = cursor.fetchone()[0]
            
            # Elasticsearch count
            es_result = self.es_client.count(index=es_index)
            es_count = es_result['count']
            
            match_percentage = (min(oracle_count, es_count) / max(oracle_count, es_count)) * 100
            
            return {
                'oracle_count': oracle_count,
                'elasticsearch_count': es_count,
                'difference': oracle_count - es_count,
                'match_percentage': match_percentage,
                'score': match_percentage,
                'status': 'PASS' if match_percentage > 95 else 'FAIL'
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'score': 0,
                'status': 'ERROR'
            }
    
    def _validate_sample_records(self, oracle_query: str, es_index: str, 
                                sample_size: int) -> Dict:
        """Validate sample records for data accuracy"""
        try:
            # Get sample from Oracle
            sample_query = f"""
                SELECT * FROM (
                    SELECT * FROM ({oracle_query}) ORDER BY DBMS_RANDOM.VALUE
                ) WHERE ROWNUM <= {sample_size}
            """
            
            cursor = self.oracle_conn.cursor()
            cursor.execute(sample_query)
            oracle_records = cursor.fetchall()
            column_names = [desc[0].lower() for desc in cursor.description]
            
            matching_records = 0
            total_checked = 0
            
            for record in oracle_records:
                oracle_doc = dict(zip(column_names, record))
                
                # Find corresponding ES document (assuming 'id' field exists)
                if 'id' in oracle_doc:
                    es_query = {
                        "query": {
                            "term": {"id": oracle_doc['id']}
                        }
                    }
                    
                    es_result = self.es_client.search(index=es_index, body=es_query, size=1)
                    
                    if es_result['hits']['total']['value'] > 0:
                        es_doc = es_result['hits']['hits'][0]['_source']
                        if self._compare_documents(oracle_doc, es_doc):
                            matching_records += 1
                    
                    total_checked += 1
            
            match_percentage = (matching_records / total_checked) * 100 if total_checked > 0 else 0
            
            return {
                'total_checked': total_checked,
                'matching_records': matching_records,
                'match_percentage': match_percentage,
                'score': match_percentage,
                'status': 'PASS' if match_percentage > 90 else 'FAIL'
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'score': 0,
                'status': 'ERROR'
            }
    
    def _validate_data_types(self, oracle_query: str, es_index: str) -> Dict:
        """Validate data type mappings"""
        try:
            # Get ES mapping
            mapping = self.es_client.indices.get_mapping(index=es_index)
            es_fields = mapping[es_index]['mappings']['properties']
            
            # Compare with Oracle schema
            cursor = self.oracle_conn.cursor()
            cursor.execute(f"SELECT * FROM ({oracle_query}) WHERE ROWNUM = 1")
            oracle_types = {desc[0].lower(): desc[1] for desc in cursor.description}
            
            type_matches = 0
            total_fields = len(oracle_types)
            
            for field_name, oracle_type in oracle_types.items():
                if field_name in es_fields:
                    es_type = es_fields[field_name].get('type', 'unknown')
                    if self._types_compatible(oracle_type, es_type):
                        type_matches += 1
            
            match_percentage = (type_matches / total_fields) * 100 if total_fields > 0 else 0
            
            return {
                'total_fields': total_fields,
                'matching_types': type_matches,
                'match_percentage': match_percentage,
                'score': match_percentage,
                'status': 'PASS' if match_percentage > 80 else 'FAIL'
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'score': 0,
                'status': 'ERROR'
            }
    
    def _check_index_health(self, es_index: str) -> Dict:
        """Check Elasticsearch index health"""
        try:
            health = self.es_client.cluster.health(index=es_index)
            stats = self.es_client.indices.stats(index=es_index)
            
            health_score = 100 if health['status'] == 'green' else (50 if health['status'] == 'yellow' else 0)
            
            return {
                'status': health['status'],
                'active_shards': health['active_shards'],
                'relocating_shards': health['relocating_shards'],
                'unassigned_shards': health['unassigned_shards'],
                'document_count': stats['indices'][es_index]['total']['docs']['count'],
                'store_size': stats['indices'][es_index]['total']['store']['size_in_bytes'],
                'score': health_score,
                'status_check': 'PASS' if health_score > 50 else 'FAIL'
            }
            
        except Exception as e:
            return {
                'error': str(e),
                'score': 0,
                'status': 'ERROR'
            }
    
    def _compare_documents(self, oracle_doc: Dict, es_doc: Dict) -> bool:
        """Compare Oracle and Elasticsearch documents for equality"""
        # Simple comparison - could be enhanced for complex data types
        for key, oracle_value in oracle_doc.items():
            if key in es_doc:
                es_value = es_doc[key]
                
                # Handle date comparisons
                if hasattr(oracle_value, 'isoformat') and isinstance(es_value, str):
                    if oracle_value.isoformat() != es_value:
                        return False
                elif oracle_value != es_value:
                    return False
            else:
                return False
        
        return True
    
    def _types_compatible(self, oracle_type, es_type: str) -> bool:
        """Check if Oracle and Elasticsearch types are compatible"""
        type_mappings = {
            oracledb.STRING: ['text', 'keyword'],
            oracledb.NUMBER: ['long', 'integer', 'double', 'float'],
            oracledb.DATETIME: ['date'],
            oracledb.CLOB: ['text'],
            oracledb.BLOB: ['binary']
        }
        
        compatible_es_types = type_mappings.get(oracle_type, [])
        return es_type in compatible_es_types