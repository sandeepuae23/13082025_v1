from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from models import OracleConnection, ElasticsearchConnection, MappingConfiguration, MigrationJob
from app import db
import logging

main_bp = Blueprint('main', __name__)
logger = logging.getLogger(__name__)

@main_bp.route('/')
def index():
    """Main dashboard"""
    try:
        # Get connection counts
        oracle_count = OracleConnection.query.filter_by(is_active=True).count()
        es_count = ElasticsearchConnection.query.filter_by(is_active=True).count()
        mapping_count = MappingConfiguration.query.filter_by(is_active=True).count()
        migration_count = MigrationJob.query.count()
        
        # Get recent migrations
        recent_migrations = MigrationJob.query.order_by(MigrationJob.created_at.desc()).limit(5).all()
        
        return render_template(
            'index.html',
            oracle_connections=oracle_count,
            es_connections=es_count,
            mappings=mapping_count,
            migration_count=migration_count,
            recent_migrations=recent_migrations,
        )
    except Exception as e:
        logger.error(f"Error loading dashboard: {str(e)}")
        flash('Error loading dashboard', 'error')
        return render_template(
            'index.html',
            oracle_connections=0,
            es_connections=0,
            mappings=0,
            migration_count=0,
            recent_migrations=[],
        )

@main_bp.route('/oracle-explorer')
def oracle_explorer():
    """Oracle database explorer"""
    return render_template('oracle_explorer.html')

@main_bp.route('/elasticsearch-explorer')
def elasticsearch_explorer():
    """Elasticsearch cluster explorer"""
    return render_template('elasticsearch_explorer.html')

@main_bp.route('/mapping-interface')
def mapping_interface():
    """Field mapping interface"""
    return render_template('mapping_interface.html')

@main_bp.route('/advanced-mapping')
def advanced_mapping():
    """Advanced field mapping interface"""
    return render_template('advanced_field_mapping.html')

@main_bp.route('/migration-status')
def migration_status():
    """Migration status monitoring"""
    return render_template('migration_status.html')
