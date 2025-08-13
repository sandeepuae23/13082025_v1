"""
Advanced Migration Routes
Handles sophisticated migration operations with real-time monitoring
"""

from flask import Blueprint, request, jsonify, render_template, current_app
import threading
import logging

from services.advanced_migration_service import AdvancedMigrationService, MigrationValidator
from models import MigrationJob, MappingConfiguration

advanced_migration_bp = Blueprint('advanced_migration', __name__)
logger = logging.getLogger(__name__)

# Global service instance for managing migrations
migration_service = None
migration_thread = None

@advanced_migration_bp.route('/deep-dive')
def deep_dive():
    """Render the migration deep dive page"""
    return render_template('migration_deep_dive.html')

@advanced_migration_bp.route('/jobs/advanced', methods=['POST'])
def start_advanced_migration():
    """Start an advanced migration with specified configuration"""
    global migration_service, migration_thread
    
    try:
        data = request.json or {}
        
        # Validate required fields
        mapping_config_id = data.get('mapping_configuration_id')
        migration_strategy = data.get('migration_strategy', 'full')
        
        if not mapping_config_id:
            return jsonify({'error': 'Mapping configuration ID is required'}), 400
        
        # Verify mapping configuration exists
        mapping_config = MappingConfiguration.query.get_or_404(mapping_config_id)
        
        # Create migration job
        job = MigrationJob(
            mapping_configuration_id=mapping_config_id,
            status='pending'
        )
        from app import db
        db.session.add(job)
        db.session.commit()
        
        # Initialize advanced migration service
        batch_size = data.get('batch_size', 5000)
        max_workers = data.get('parallel_workers', 4)
        
        migration_service = AdvancedMigrationService(
            batch_size=batch_size,
            max_workers=max_workers
        )
        
        # Start migration in background thread with proper app context
        flask_app = current_app._get_current_object()

        def run_migration():
            with flask_app.app_context():
                migration_service.start_advanced_migration(job.id, migration_strategy)

        migration_thread = threading.Thread(target=run_migration, daemon=True)
        migration_thread.start()
        
        return jsonify({
            'job_id': job.id,
            'message': f'Advanced {migration_strategy} migration started successfully',
            'configuration': {
                'batch_size': batch_size,
                'parallel_workers': max_workers,
                'strategy': migration_strategy
            }
        })
        
    except Exception as e:
        logger.error(f"Error starting advanced migration: {str(e)}")
        return jsonify({'error': str(e)}), 500

@advanced_migration_bp.route('/jobs/<int:job_id>/metrics', methods=['GET'])
def get_migration_metrics(job_id):
    """Get real-time migration metrics"""
    global migration_service
    
    try:
        if not migration_service:
            return jsonify({'error': 'No active migration service'}), 404
        
        metrics = migration_service.get_metrics()
        
        # Add job-specific information
        job = MigrationJob.query.get_or_404(job_id)
        metrics.update({
            'job_id': job.id,
            'job_status': job.status,
            'mapping_configuration': job.mapping_configuration.name,
            'elasticsearch_index': job.mapping_configuration.elasticsearch_index
        })
        
        return jsonify(metrics)
        
    except Exception as e:
        logger.error(f"Error fetching migration metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@advanced_migration_bp.route('/jobs/<int:job_id>/validate', methods=['POST'])
def validate_migration(job_id):
    """Run comprehensive migration validation"""
    try:
        job = MigrationJob.query.get_or_404(job_id)
        mapping_config = job.mapping_configuration
        
        # Create Oracle and Elasticsearch connections
        oracle_conn = migration_service._create_oracle_connection(mapping_config.oracle_connection)
        es_client = migration_service._create_elasticsearch_client(mapping_config.elasticsearch_connection)
        
        # Initialize validator
        validator = MigrationValidator(oracle_conn, es_client)
        
        # Run validation
        validation_results = validator.validate_migration(
            mapping_config.oracle_query,
            mapping_config.elasticsearch_index,
            sample_size=1000
        )
        
        # Clean up connections
        oracle_conn.close()
        
        return jsonify(validation_results)
        
    except Exception as e:
        logger.error(f"Error validating migration: {str(e)}")
        return jsonify({'error': str(e)}), 500

@advanced_migration_bp.route('/jobs/<int:job_id>/stop', methods=['POST'])
def stop_migration(job_id):
    """Stop a running migration"""
    global migration_service
    
    try:
        if migration_service:
            migration_service.stop_migration()
            
        # Update job status
        job = MigrationJob.query.get_or_404(job_id)
        job.status = 'stopped'
        from app import db
        db.session.commit()
        
        return jsonify({'message': 'Migration stop requested'})
        
    except Exception as e:
        logger.error(f"Error stopping migration: {str(e)}")
        return jsonify({'error': str(e)}), 500

@advanced_migration_bp.route('/reprocess-failed', methods=['POST'])
def reprocess_failed_records():
    """Reprocess failed records from dead letter queue"""
    global migration_service
    
    try:
        if not migration_service:
            return jsonify({'error': 'No active migration service'}), 404
        
        data = request.json or {}
        table_name = data.get('table_name')
        
        result = migration_service.reprocess_failed_records(table_name)
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error reprocessing failed records: {str(e)}")
        return jsonify({'error': str(e)}), 500

@advanced_migration_bp.route('/performance/recommendations', methods=['GET'])
def get_performance_recommendations():
    """Get performance optimization recommendations"""
    try:
        # This would analyze current system resources and provide recommendations
        recommendations = {
            'batch_size': {
                'current': 5000,
                'recommended': 7500,
                'reason': 'Based on available memory and ES cluster capacity'
            },
            'parallel_workers': {
                'current': 4,
                'recommended': 6,
                'reason': 'CPU cores available for parallel processing'
            },
            'es_settings': {
                'refresh_interval': '30s',
                'number_of_replicas': 0,
                'translog_durability': 'async',
                'reason': 'Optimize for bulk loading performance'
            },
            'oracle_settings': {
                'arraysize': 10000,
                'prefetchrows': 1000,
                'reason': 'Reduce network round trips'
            }
        }
        
        return jsonify(recommendations)
        
    except Exception as e:
        logger.error(f"Error getting performance recommendations: {str(e)}")
        return jsonify({'error': str(e)}), 500

@advanced_migration_bp.route('/data-types/analysis', methods=['POST'])
def analyze_data_types():
    """Analyze Oracle data types and suggest Elasticsearch mappings"""
    try:
        data = request.json or {}
        oracle_connection_id = data.get('oracle_connection_id')
        oracle_query = data.get('oracle_query')
        
        if not oracle_connection_id or not oracle_query:
            return jsonify({'error': 'Oracle connection ID and query are required'}), 400
        
        # This would analyze the Oracle query and suggest optimal ES mappings
        # For now, return sample analysis
        analysis = {
            'fields': [
                {
                    'oracle_name': 'CUSTOMER_ID',
                    'oracle_type': 'NUMBER(10)',
                    'suggested_es_type': 'long',
                    'confidence': 95,
                    'reasoning': 'Integer primary key, use long for large values'
                },
                {
                    'oracle_name': 'CUSTOMER_NAME',
                    'oracle_type': 'VARCHAR2(100)',
                    'suggested_es_type': 'text',
                    'suggested_es_fields': {
                        'keyword': {'type': 'keyword', 'ignore_above': 256}
                    },
                    'confidence': 90,
                    'reasoning': 'Text field with keyword sub-field for exact matching'
                },
                {
                    'oracle_name': 'ORDER_DATE',
                    'oracle_type': 'TIMESTAMP',
                    'suggested_es_type': 'date',
                    'suggested_format': 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
                    'confidence': 100,
                    'reasoning': 'Direct mapping with flexible date formats'
                },
                {
                    'oracle_name': 'ORDER_AMOUNT',
                    'oracle_type': 'NUMBER(10,2)',
                    'suggested_es_type': 'scaled_float',
                    'suggested_scaling_factor': 100,
                    'confidence': 85,
                    'reasoning': 'Financial data with 2 decimal places, use scaled_float for precision'
                }
            ],
            'summary': {
                'total_fields': 4,
                'high_confidence': 3,
                'medium_confidence': 1,
                'low_confidence': 0,
                'potential_issues': [
                    'Consider using scaled_float for financial amounts to maintain precision'
                ]
            }
        }
        
        return jsonify(analysis)
        
    except Exception as e:
        logger.error(f"Error analyzing data types: {str(e)}")
        return jsonify({'error': str(e)}), 500

@advanced_migration_bp.route('/transformation-rules/suggest', methods=['POST'])
def suggest_transformation_rules():
    """Suggest transformation rules based on data analysis"""
    try:
        data = request.json or {}
        source_field = data.get('source_field')
        source_type = data.get('source_type')
        target_type = data.get('target_type')
        sample_values = data.get('sample_values', [])
        
        # Analyze sample values and suggest transformations
        suggestions = []
        
        if source_type == 'TIMESTAMP' and target_type == 'date':
            suggestions.append({
                'type': 'date_format',
                'description': 'Convert Oracle TIMESTAMP to ISO 8601 format',
                'configuration': {
                    'from_format': '%Y-%m-%d %H:%M:%S',
                    'to_format': '%Y-%m-%dT%H:%M:%SZ'
                },
                'confidence': 95
            })
        
        if source_type.startswith('VARCHAR') and target_type == 'text':
            suggestions.append({
                'type': 'string_cleanup',
                'description': 'Trim whitespace and normalize text',
                'configuration': {
                    'operations': ['trim', 'normalize_unicode']
                },
                'confidence': 80
            })
        
        if source_type.startswith('NUMBER') and target_type in ['scaled_float', 'double']:
            suggestions.append({
                'type': 'numeric_validation',
                'description': 'Validate numeric ranges and handle null values',
                'configuration': {
                    'null_handling': 'skip',
                    'range_validation': True
                },
                'confidence': 85
            })
        
        return jsonify({
            'field': source_field,
            'suggestions': suggestions
        })
        
    except Exception as e:
        logger.error(f"Error suggesting transformation rules: {str(e)}")
        return jsonify({'error': str(e)}), 500