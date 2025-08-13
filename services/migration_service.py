import logging
import threading
import time
from datetime import datetime
from models import MigrationJob, MappingConfiguration
from services.oracle_service import OracleService
from services.elasticsearch_service import ElasticsearchService
from app import db

logger = logging.getLogger(__name__)

class MigrationService:
    def __init__(self):
        self.running_jobs = {}
        self.stop_flags = {}
    
    def start_migration(self, job_id):
        """Start migration job in background thread"""
        if job_id in self.running_jobs:
            logger.warning(f"Migration job {job_id} is already running")
            return
        
        self.stop_flags[job_id] = False
        thread = threading.Thread(target=self._execute_migration, args=(job_id,))
        thread.daemon = True
        thread.start()
        self.running_jobs[job_id] = thread
    
    def stop_migration(self, job_id):
        """Stop running migration job"""
        if job_id in self.stop_flags:
            self.stop_flags[job_id] = True
            logger.info(f"Stop signal sent for migration job {job_id}")
    
    def _execute_migration(self, job_id):
        """Execute the actual data migration"""
        try:
            with db.app.app_context():
                # Get job and mapping configuration
                job = MigrationJob.query.get(job_id)
                if not job:
                    logger.error(f"Migration job {job_id} not found")
                    return
                
                mapping_config = job.mapping_configuration
                
                # Update job status
                job.status = 'running'
                job.start_time = datetime.utcnow()
                db.session.commit()
                
                logger.info(f"Starting migration job {job_id}")
                
                # Initialize services
                oracle_service = OracleService(mapping_config.oracle_connection)
                es_service = ElasticsearchService(mapping_config.elasticsearch_connection)
                
                # Get total record count
                total_records = self._get_total_record_count(oracle_service, mapping_config.oracle_query)
                job.total_records = total_records
                db.session.commit()
                
                # Process data in batches
                batch_size = 1000
                processed = 0
                failed = 0
                
                for batch in self._get_data_batches(oracle_service, mapping_config.oracle_query, batch_size):
                    # Check stop flag
                    if self.stop_flags.get(job_id, False):
                        job.status = 'stopped'
                        job.end_time = datetime.utcnow()
                        db.session.commit()
                        logger.info(f"Migration job {job_id} stopped by user")
                        return
                    
                    # Transform data according to mappings
                    transformed_data = self._transform_batch(batch, mapping_config)
                    
                    # Index to Elasticsearch
                    try:
                        result = es_service.bulk_index(mapping_config.elasticsearch_index, transformed_data)
                        processed += result['success_count']
                        failed += result['failed_count']
                        
                        if result['errors']:
                            logger.warning(f"Batch errors in job {job_id}: {result['errors']}")
                        
                    except Exception as e:
                        logger.error(f"Error indexing batch in job {job_id}: {str(e)}")
                        failed += len(transformed_data)
                    
                    # Update progress
                    job.processed_records = processed
                    job.failed_records = failed
                    db.session.commit()
                    
                    # Small delay to prevent overwhelming the systems
                    time.sleep(0.1)
                
                # Complete the job
                job.status = 'completed'
                job.end_time = datetime.utcnow()
                db.session.commit()
                
                logger.info(f"Migration job {job_id} completed. Processed: {processed}, Failed: {failed}")
                
        except Exception as e:
            logger.error(f"Migration job {job_id} failed: {str(e)}")
            with db.app.app_context():
                job = MigrationJob.query.get(job_id)
                if job:
                    job.status = 'failed'
                    job.error_message = str(e)
                    job.end_time = datetime.utcnow()
                    db.session.commit()
        
        finally:
            # Clean up
            if job_id in self.running_jobs:
                del self.running_jobs[job_id]
            if job_id in self.stop_flags:
                del self.stop_flags[job_id]
    
    def _get_total_record_count(self, oracle_service, query):
        """Get total number of records that will be migrated"""
        try:
            count_query = f"SELECT COUNT(*) FROM ({query})"
            result = oracle_service.execute_query(count_query, limit=1)
            return result['rows'][0]['COUNT(*)'] if result['rows'] else 0
        except Exception as e:
            logger.error(f"Error getting record count: {str(e)}")
            return 0
    
    def _get_data_batches(self, oracle_service, query, batch_size):
        """Generator that yields data in batches"""
        offset = 0
        
        while True:
            # Oracle uses ROWNUM for pagination
            paginated_query = f"""
                SELECT * FROM (
                    SELECT rownum rn, t.* FROM ({query}) t
                ) WHERE rn > {offset} AND rn <= {offset + batch_size}
            """
            
            try:
                result = oracle_service.execute_query(paginated_query, limit=batch_size + 1)
                
                if not result['rows']:
                    break
                
                # Remove the rownum column
                batch_data = []
                for row in result['rows']:
                    if 'RN' in row:
                        del row['RN']
                    batch_data.append(row)
                
                yield batch_data
                
                if len(batch_data) < batch_size:
                    break
                
                offset += batch_size
                
            except Exception as e:
                logger.error(f"Error fetching batch at offset {offset}: {str(e)}")
                break
    
    def _transform_batch(self, batch_data, mapping_config):
        """Transform batch data according to field mappings"""
        field_mappings = mapping_config.get_field_mappings()
        transformation_rules = mapping_config.get_transformation_rules()
        
        transformed_batch = []
        
        for row in batch_data:
            transformed_row = {}
            
            # Apply field mappings
            for mapping in field_mappings:
                oracle_field = mapping.get('oracle_field')
                es_field = mapping.get('es_field')
                
                if oracle_field in row and es_field:
                    value = row[oracle_field]
                    
                    # Apply transformations
                    for rule in transformation_rules:
                        if rule.get('target') == oracle_field:
                            value = self._apply_transformation(value, rule)
                    
                    # Handle nested field assignment
                    self._set_nested_value(transformed_row, es_field, value)
            
            transformed_batch.append(transformed_row)
        
        return transformed_batch
    
    def _apply_transformation(self, value, rule):
        """Apply transformation rule to a value"""
        rule_type = rule.get('rule')
        
        if rule_type == 'FORMAT_DATE' and value:
            # Convert Oracle date to ISO format
            if hasattr(value, 'isoformat'):
                return value.isoformat()
            return str(value)
        
        elif rule_type == 'CAST_FLOAT' and value is not None:
            try:
                return float(value)
            except (ValueError, TypeError):
                return value
        
        elif rule_type == 'TRIM_SPACES' and isinstance(value, str):
            return value.strip()
        
        return value
    
    def _set_nested_value(self, obj, path, value):
        """Set value in nested object using dot notation"""
        parts = path.split('.')
        current = obj
        
        for i, part in enumerate(parts):
            if i == len(parts) - 1:  # Last part
                current[part] = value
            else:
                if part not in current:
                    current[part] = {}
                current = current[part]
    
    def preview_migration(self, mapping_config, limit=5):
        """Preview migration results with sample data"""
        try:
            # Initialize services
            oracle_service = OracleService(mapping_config.oracle_connection)
            
            # Get sample data
            sample_data = oracle_service.execute_query(mapping_config.oracle_query, limit)
            
            # Transform sample data
            transformed_data = self._transform_batch(sample_data['rows'], mapping_config)
            
            return {
                'original_data': sample_data['rows'],
                'transformed_data': transformed_data,
                'field_mappings': mapping_config.get_field_mappings(),
                'transformation_rules': mapping_config.get_transformation_rules()
            }
            
        except Exception as e:
            logger.error(f"Error previewing migration: {str(e)}")
            raise
