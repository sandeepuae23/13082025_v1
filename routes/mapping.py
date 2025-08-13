from flask import Blueprint, request, jsonify, render_template
from models import MappingConfiguration, OracleConnection, ElasticsearchConnection
from services.mapping_service import MappingService
from services.advanced_mapping_service import AdvancedMappingService
from services.oracle_service import OracleService
from app import db
import logging
import json

mapping_bp = Blueprint('mapping', __name__)
logger = logging.getLogger(__name__)

@mapping_bp.route('/configurations', methods=['GET'])
def get_configurations():
    """Get all mapping configurations"""
    try:
        configs = MappingConfiguration.query.filter_by(is_active=True).all()
        return jsonify([{
            'id': config.id,
            'name': config.name,
            'oracle_connection': config.oracle_connection.name,
            'elasticsearch_connection': config.elasticsearch_connection.name,
            'elasticsearch_index': config.elasticsearch_index,
            'created_at': config.created_at.isoformat(),
            'updated_at': config.updated_at.isoformat()
        } for config in configs])
    except Exception as e:
        logger.error(f"Error fetching mapping configurations: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/configurations', methods=['POST'])
def create_configuration():
    """Create a new mapping configuration"""
    try:
        data = request.json or {}
        config = MappingConfiguration(
            name=data.get('name', ''),
            oracle_connection_id=data.get('oracle_connection_id'),
            elasticsearch_connection_id=data.get('elasticsearch_connection_id'),
            oracle_query=data.get('oracle_query', ''),
            elasticsearch_index=data.get('elasticsearch_index', '')
        )
        config.set_field_mappings(data.get('field_mappings', []))
        config.set_transformation_rules(data.get('transformation_rules', []))
        
        db.session.add(config)
        db.session.commit()
        
        return jsonify({'id': config.id, 'message': 'Configuration created successfully'})
    except Exception as e:
        logger.error(f"Error creating mapping configuration: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/configurations/<int:config_id>', methods=['GET'])
def get_configuration(config_id):
    """Get a specific mapping configuration"""
    try:
        config = MappingConfiguration.query.get_or_404(config_id)
        return jsonify({
            'id': config.id,
            'name': config.name,
            'oracle_connection_id': config.oracle_connection_id,
            'elasticsearch_connection_id': config.elasticsearch_connection_id,
            'oracle_query': config.oracle_query,
            'elasticsearch_index': config.elasticsearch_index,
            'field_mappings': config.get_field_mappings(),
            'transformation_rules': config.get_transformation_rules(),
            'created_at': config.created_at.isoformat(),
            'updated_at': config.updated_at.isoformat()
        })
    except Exception as e:
        logger.error(f"Error fetching mapping configuration: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/configurations/<int:config_id>', methods=['PUT'])
def update_configuration(config_id):
    """Update a mapping configuration"""
    try:
        config = MappingConfiguration.query.get_or_404(config_id)
        data = request.json
        
        config.name = data.get('name', config.name)
        config.oracle_query = data.get('oracle_query', config.oracle_query)
        config.elasticsearch_index = data.get('elasticsearch_index', config.elasticsearch_index)
        
        if 'field_mappings' in data:
            config.set_field_mappings(data['field_mappings'])
        if 'transformation_rules' in data:
            config.set_transformation_rules(data['transformation_rules'])
        
        db.session.commit()
        return jsonify({'message': 'Configuration updated successfully'})
    except Exception as e:
        logger.error(f"Error updating mapping configuration: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/auto-suggest', methods=['POST'])
def auto_suggest_mapping():
    """Generate automatic mapping suggestions"""
    try:
        data = request.json
        oracle_connection_id = data['oracle_connection_id']
        elasticsearch_connection_id = data['elasticsearch_connection_id']
        oracle_query = data['oracle_query']
        elasticsearch_index = data['elasticsearch_index']
        
        oracle_conn = OracleConnection.query.get_or_404(oracle_connection_id)
        es_conn = ElasticsearchConnection.query.get_or_404(elasticsearch_connection_id)
        
        mapping_service = MappingService(oracle_conn, es_conn)
        suggestions = mapping_service.generate_auto_mapping(oracle_query, elasticsearch_index)
        
        return jsonify(suggestions)
    except Exception as e:
        logger.error(f"Error generating auto mapping: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/validate', methods=['POST'])
def validate_mapping():
    """Validate field mappings and type compatibility"""
    try:
        data = request.json
        oracle_connection_id = data['oracle_connection_id']
        elasticsearch_connection_id = data['elasticsearch_connection_id']
        field_mappings = data['field_mappings']
        
        oracle_conn = OracleConnection.query.get_or_404(oracle_connection_id)
        es_conn = ElasticsearchConnection.query.get_or_404(elasticsearch_connection_id)
        
        mapping_service = MappingService(oracle_conn, es_conn)
        validation_result = mapping_service.validate_mappings(field_mappings)
        
        return jsonify(validation_result)
    except Exception as e:
        logger.error(f"Error validating mapping: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/export/<int:config_id>', methods=['GET'])
def export_configuration(config_id):
    """Export mapping configuration as JSON"""
    try:
        config = MappingConfiguration.query.get_or_404(config_id)
        export_data = {
            'name': config.name,
            'oracle_query': config.oracle_query,
            'elasticsearch_index': config.elasticsearch_index,
            'field_mappings': config.get_field_mappings(),
            'transformation_rules': config.get_transformation_rules(),
            'exported_at': config.updated_at.isoformat()
        }
        return jsonify(export_data)
    except Exception as e:
        logger.error(f"Error exporting configuration: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/import', methods=['POST'])
def import_configuration():
    """Import mapping configuration from JSON"""
    try:
        data = request.json
        
        config = MappingConfiguration(
            name=data['name'],
            oracle_connection_id=data['oracle_connection_id'],
            elasticsearch_connection_id=data['elasticsearch_connection_id'],
            oracle_query=data['oracle_query'],
            elasticsearch_index=data['elasticsearch_index']
        )
        config.set_field_mappings(data.get('field_mappings', []))
        config.set_transformation_rules(data.get('transformation_rules', []))
        
        db.session.add(config)
        db.session.commit()
        
        return jsonify({'id': config.id, 'message': 'Configuration imported successfully'})
    except Exception as e:
        logger.error(f"Error importing configuration: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/advanced-interface')
def advanced_interface():
    """Render the advanced field mapping interface"""
    return render_template('advanced_field_mapping.html')

@mapping_bp.route('/analyze-schema', methods=['POST'])
def analyze_schema():
    """Analyze Oracle schema for advanced mapping suggestions"""
    try:
        data = request.json or {}
        oracle_connection_id = data.get('oracle_connection_id')
        oracle_query = data.get('oracle_query')
        
        if not oracle_connection_id or not oracle_query:
            return jsonify({'error': 'Oracle connection ID and query are required'}), 400
        
        # Get Oracle connection and create service
        oracle_connection = OracleConnection.query.get_or_404(oracle_connection_id)
        oracle_service = OracleService(oracle_connection)
        
        # Initialize advanced mapping service
        advanced_mapping_service = AdvancedMappingService()
        
        # Analyze schema
        analysis = advanced_mapping_service.analyze_oracle_schema(oracle_service, oracle_query)
        
        return jsonify(analysis)
        
    except Exception as e:
        logger.error(f"Error analyzing schema: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/configurations/advanced', methods=['POST'])
def create_advanced_configuration():
    """Create advanced mapping configuration with nested and parent-child support"""
    try:
        data = request.json or {}
        
        # Create base configuration
        base_config = MappingConfiguration(
            name=data.get('name', ''),
            oracle_connection_id=data.get('oracle_connection_id'),
            elasticsearch_connection_id=data.get('elasticsearch_connection_id'),
            oracle_query=data.get('oracle_query', ''),
            elasticsearch_index=data.get('elasticsearch_index', '')
        )
        
        # Store advanced mapping data
        advanced_data = {
            'mapping_strategy': data.get('mapping_strategy', 'direct'),
            'field_mappings': data.get('field_mappings', []),
            'nested_mappings': data.get('nested_mappings', []),
            'parent_child_mappings': data.get('parent_child_mappings', []),
            'transformation_rules': data.get('transformation_rules', [])
        }
        
        base_config.set_field_mappings(advanced_data['field_mappings'])
        base_config.set_transformation_rules(advanced_data['transformation_rules'])
        
        # Store additional advanced configuration
        advanced_metadata = {
            'mapping_strategy': advanced_data['mapping_strategy'],
            'nested_mappings': advanced_data['nested_mappings'],
            'parent_child_mappings': advanced_data['parent_child_mappings']
        }
        
        # Add to mapping_metadata field
        base_config.set_mapping_metadata(advanced_metadata)
        
        db.session.add(base_config)
        db.session.commit()
        
        return jsonify({'id': base_config.id, 'message': 'Advanced configuration created successfully'})
        
    except Exception as e:
        logger.error(f"Error creating advanced configuration: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/generate-elasticsearch-mapping', methods=['POST'])
def generate_elasticsearch_mapping():
    """Generate Elasticsearch mapping from advanced configuration"""
    try:
        data = request.json or {}
        
        # Initialize advanced mapping service
        advanced_mapping_service = AdvancedMappingService()
        
        # Load configuration data
        field_mappings = data.get('field_mappings', [])
        nested_mappings = data.get('nested_mappings', [])
        parent_child_mappings = data.get('parent_child_mappings', [])
        
        # Create nested mappings
        for nested_config in nested_mappings:
            advanced_mapping_service.create_nested_mapping(
                nested_config.get('parent_table', ''),
                nested_config
            )
        
        # Create parent-child mappings
        for pc_config in parent_child_mappings:
            advanced_mapping_service.create_parent_child_mapping(pc_config)
        
        # Generate final Elasticsearch mapping
        es_mapping = advanced_mapping_service.generate_elasticsearch_mapping()
        
        return jsonify(es_mapping)
        
    except Exception as e:
        logger.error(f"Error generating Elasticsearch mapping: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/transformation-query', methods=['POST'])
def generate_transformation_query():
    """Generate transformation query for complex mappings"""
    try:
        data = request.json or {}
        oracle_query = data.get('oracle_query', '')
        
        if not oracle_query:
            return jsonify({'error': 'Oracle query is required'}), 400
        
        # Initialize advanced mapping service
        advanced_mapping_service = AdvancedMappingService()
        
        # Load configuration data
        nested_mappings = data.get('nested_mappings', [])
        parent_child_mappings = data.get('parent_child_mappings', [])
        
        # Create mappings in service
        for nested_config in nested_mappings:
            advanced_mapping_service.create_nested_mapping(
                nested_config.get('parent_table', ''),
                nested_config
            )
        
        for pc_config in parent_child_mappings:
            advanced_mapping_service.create_parent_child_mapping(pc_config)
        
        # Generate transformation query
        transformation = advanced_mapping_service.generate_transformation_query(oracle_query)
        
        return jsonify(transformation)
        
    except Exception as e:
        logger.error(f"Error generating transformation query: {str(e)}")
        return jsonify({'error': str(e)}), 500
