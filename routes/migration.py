from flask import Blueprint, request, jsonify
from models import MigrationJob, MappingConfiguration
from services.migration_service import MigrationService
from app import db
import logging

migration_bp = Blueprint('migration', __name__)
logger = logging.getLogger(__name__)

@migration_bp.route('/jobs', methods=['GET'])
def get_jobs():
    """Get all migration jobs"""
    try:
        jobs = MigrationJob.query.order_by(MigrationJob.created_at.desc()).all()
        return jsonify([{
            'id': job.id,
            'mapping_configuration_name': job.mapping_configuration.name,
            'status': job.status,
            'total_records': job.total_records,
            'processed_records': job.processed_records,
            'failed_records': job.failed_records,
            'progress_percentage': job.progress_percentage,
            'start_time': job.start_time.isoformat() if job.start_time else None,
            'end_time': job.end_time.isoformat() if job.end_time else None,
            'error_message': job.error_message,
            'created_at': job.created_at.isoformat()
        } for job in jobs])
    except Exception as e:
        logger.error(f"Error fetching migration jobs: {str(e)}")
        return jsonify({'error': str(e)}), 500

@migration_bp.route('/jobs', methods=['POST'])
def create_job():
    """Create and start a new migration job"""
    try:
        data = request.json or {}
        mapping_config_id = data.get('mapping_configuration_id')
        
        if not mapping_config_id:
            return jsonify({'error': 'Mapping configuration ID is required'}), 400
            
        mapping_config = MappingConfiguration.query.get_or_404(mapping_config_id)
        
        # Create new migration job
        job = MigrationJob(
            mapping_configuration_id=mapping_config_id,
            status='pending'
        )
        db.session.add(job)
        db.session.commit()
        
        # Start migration in background
        migration_service = MigrationService()
        migration_service.start_migration(job.id)
        
        return jsonify({'job_id': job.id, 'message': 'Migration job started successfully'})
    except Exception as e:
        logger.error(f"Error creating migration job: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@migration_bp.route('/jobs/<int:job_id>', methods=['GET'])
def get_job(job_id):
    """Get a specific migration job"""
    try:
        job = MigrationJob.query.get_or_404(job_id)
        return jsonify({
            'id': job.id,
            'mapping_configuration_id': job.mapping_configuration_id,
            'mapping_configuration_name': job.mapping_configuration.name,
            'status': job.status,
            'total_records': job.total_records,
            'processed_records': job.processed_records,
            'failed_records': job.failed_records,
            'progress_percentage': job.progress_percentage,
            'start_time': job.start_time.isoformat() if job.start_time else None,
            'end_time': job.end_time.isoformat() if job.end_time else None,
            'error_message': job.error_message,
            'created_at': job.created_at.isoformat()
        })
    except Exception as e:
        logger.error(f"Error fetching migration job: {str(e)}")
        return jsonify({'error': str(e)}), 500

@migration_bp.route('/jobs/<int:job_id>/stop', methods=['POST'])
def stop_job(job_id):
    """Stop a running migration job"""
    try:
        job = MigrationJob.query.get_or_404(job_id)
        
        if job.status != 'running':
            return jsonify({'error': 'Job is not running'}), 400
        
        migration_service = MigrationService()
        migration_service.stop_migration(job_id)
        
        return jsonify({'message': 'Migration job stopped'})
    except Exception as e:
        logger.error(f"Error stopping migration job: {str(e)}")
        return jsonify({'error': str(e)}), 500

@migration_bp.route('/jobs/<int:job_id>/retry', methods=['POST'])
def retry_job(job_id):
    """Retry a failed migration job"""
    try:
        job = MigrationJob.query.get_or_404(job_id)
        
        if job.status != 'failed':
            return jsonify({'error': 'Job has not failed'}), 400
        
        # Reset job status
        job.status = 'pending'
        job.processed_records = 0
        job.failed_records = 0
        job.start_time = None
        job.end_time = None
        job.error_message = None
        db.session.commit()
        
        # Restart migration
        migration_service = MigrationService()
        migration_service.start_migration(job_id)
        
        return jsonify({'message': 'Migration job restarted'})
    except Exception as e:
        logger.error(f"Error retrying migration job: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@migration_bp.route('/preview', methods=['POST'])
def preview_migration():
    """Preview migration results with sample data"""
    try:
        data = request.json
        mapping_config_id = data['mapping_configuration_id']
        limit = data.get('limit', 5)
        
        mapping_config = MappingConfiguration.query.get_or_404(mapping_config_id)
        
        migration_service = MigrationService()
        preview_data = migration_service.preview_migration(mapping_config, limit)
        
        return jsonify(preview_data)
    except Exception as e:
        logger.error(f"Error previewing migration: {str(e)}")
        return jsonify({'error': str(e)}), 500
