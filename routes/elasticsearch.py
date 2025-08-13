from flask import Blueprint, request, jsonify
from models import ElasticsearchConnection
from services.elasticsearch_service import ElasticsearchService
from app import db
import logging

elasticsearch_bp = Blueprint('elasticsearch', __name__)
logger = logging.getLogger(__name__)

@elasticsearch_bp.route('/connections', methods=['GET'])
def get_connections():
    """Get all Elasticsearch connections"""
    try:
        connections = ElasticsearchConnection.query.filter_by(is_active=True).all()
        return jsonify([{
            'id': conn.id,
            'name': conn.name,
            'environment': conn.environment,
            'host': conn.host,
            'port': conn.port,
            'username': conn.username,
            'use_ssl': conn.use_ssl,
            'created_at': conn.created_at.isoformat()
        } for conn in connections])
    except Exception as e:
        logger.error(f"Error fetching Elasticsearch connections: {str(e)}")
        return jsonify({'error': str(e)}), 500

@elasticsearch_bp.route('/connections', methods=['POST'])
def create_connection():
    """Create a new Elasticsearch connection"""
    try:
        data = request.json or {}
        connection = ElasticsearchConnection(
            name=data.get('name', ''),
            environment=data.get('environment', ''),
            host=data.get('host', ''),
            port=data.get('port', 9200),
            username=data.get('username'),
            password=data.get('password'),
            use_ssl=data.get('use_ssl', False)
        )
        db.session.add(connection)
        db.session.commit()
        
        return jsonify({'id': connection.id, 'message': 'Connection created successfully'})
    except Exception as e:
        logger.error(f"Error creating Elasticsearch connection: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@elasticsearch_bp.route('/connections/<int:connection_id>/test', methods=['POST'])
def test_connection(connection_id):
    """Test Elasticsearch connection"""
    try:
        connection = ElasticsearchConnection.query.get_or_404(connection_id)
        es_service = ElasticsearchService(connection)
        
        if es_service.test_connection():
            return jsonify({'success': True, 'message': 'Connection successful'})
        else:
            return jsonify({'success': False, 'message': 'Connection failed'}), 400
    except Exception as e:
        logger.error(f"Error testing Elasticsearch connection: {str(e)}")
        return jsonify({'error': str(e)}), 500

@elasticsearch_bp.route('/connections/<int:connection_id>/indices', methods=['GET'])
def get_indices(connection_id):
    """Get all indices from Elasticsearch cluster"""
    try:
        connection = ElasticsearchConnection.query.get_or_404(connection_id)
        es_service = ElasticsearchService(connection)
        
        indices = es_service.get_indices()
        return jsonify(indices)
    except Exception as e:
        logger.error(f"Error fetching Elasticsearch indices: {str(e)}")
        return jsonify({'error': str(e)}), 500

@elasticsearch_bp.route('/connections/<int:connection_id>/indices/<index_name>/mapping', methods=['GET'])
def get_index_mapping(connection_id, index_name):
    """Get mapping for a specific index"""
    try:
        connection = ElasticsearchConnection.query.get_or_404(connection_id)
        es_service = ElasticsearchService(connection)
        
        mapping = es_service.get_index_mapping(index_name)
        return jsonify(mapping)
    except Exception as e:
        logger.error(f"Error fetching index mapping: {str(e)}")
        return jsonify({'error': str(e)}), 500

@elasticsearch_bp.route('/connections/<int:connection_id>/indices', methods=['POST'])
def create_index(connection_id):
    """Create a new Elasticsearch index"""
    try:
        connection = ElasticsearchConnection.query.get_or_404(connection_id)
        es_service = ElasticsearchService(connection)
        
        data = request.json or {}
        index_name = data.get('index_name', '')
        mapping = data.get('mapping', {})
        
        result = es_service.create_index(index_name, mapping)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error creating Elasticsearch index: {str(e)}")
        return jsonify({'error': str(e)}), 500

@elasticsearch_bp.route('/connections/<int:connection_id>/indices/<index_name>/fields', methods=['GET'])
def get_index_fields(connection_id, index_name):
    """Get all fields from an Elasticsearch index"""
    try:
        connection = ElasticsearchConnection.query.get_or_404(connection_id)
        es_service = ElasticsearchService(connection)
        
        fields = es_service.get_index_fields(index_name)
        return jsonify(fields)
    except Exception as e:
        logger.error(f"Error fetching index fields: {str(e)}")
        return jsonify({'error': str(e)}), 500
