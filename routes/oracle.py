from flask import Blueprint, request, jsonify
from models import OracleConnection
from services.oracle_service import OracleService
from app import db
import logging

oracle_bp = Blueprint('oracle', __name__)
logger = logging.getLogger(__name__)

@oracle_bp.route('/connections', methods=['GET'])
def get_connections():
    """Get all Oracle connections"""
    try:
        connections = OracleConnection.query.filter_by(is_active=True).all()
        return jsonify([{
            'id': conn.id,
            'name': conn.name,
            'host': conn.host,
            'port': conn.port,
            'service_name': conn.service_name,
            'username': conn.username,
            'created_at': conn.created_at.isoformat()
        } for conn in connections])
    except Exception as e:
        logger.error(f"Error fetching Oracle connections: {str(e)}")
        return jsonify({'error': str(e)}), 500

@oracle_bp.route('/connections', methods=['POST'])
def create_connection():
    """Create a new Oracle connection"""
    try:
        data = request.json or {}
        connection = OracleConnection(
            name=data.get('name', ''),
            host=data.get('host', ''),
            port=data.get('port', 1521),
            service_name=data.get('service_name', ''),
            username=data.get('username', ''),
            password=data.get('password', '')
        )
        db.session.add(connection)
        db.session.commit()
        
        return jsonify({'id': connection.id, 'message': 'Connection created successfully'})
    except Exception as e:
        logger.error(f"Error creating Oracle connection: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@oracle_bp.route('/connections/<int:connection_id>/test', methods=['POST'])
def test_connection(connection_id):
    """Test Oracle connection"""
    try:
        connection = OracleConnection.query.get_or_404(connection_id)
        oracle_service = OracleService(connection)
        
        if oracle_service.test_connection():
            return jsonify({'success': True, 'message': 'Connection successful'})
        else:
            return jsonify({'success': False, 'message': 'Connection failed'}), 400
    except Exception as e:
        logger.error(f"Error testing Oracle connection: {str(e)}")
        return jsonify({'error': str(e)}), 500

@oracle_bp.route('/connections/<int:connection_id>/tables', methods=['GET'])
def get_tables(connection_id):
    """Get all tables from Oracle connection"""
    try:
        connection = OracleConnection.query.get_or_404(connection_id)
        oracle_service = OracleService(connection)
        
        tables = oracle_service.get_tables()
        return jsonify(tables)
    except Exception as e:
        logger.error(f"Error fetching Oracle tables: {str(e)}")
        return jsonify({'error': str(e)}), 500

@oracle_bp.route('/connections/<int:connection_id>/tables/<table_name>/columns', methods=['GET'])
def get_table_columns(connection_id, table_name):
    """Get columns for a specific table"""
    try:
        connection = OracleConnection.query.get_or_404(connection_id)
        oracle_service = OracleService(connection)
        
        columns = oracle_service.get_table_columns(table_name)
        return jsonify(columns)
    except Exception as e:
        logger.error(f"Error fetching table columns: {str(e)}")
        return jsonify({'error': str(e)}), 500

@oracle_bp.route('/connections/<int:connection_id>/query/analyze', methods=['POST'])
def analyze_query(connection_id):
    """Analyze SQL query and extract column information"""
    try:
        connection = OracleConnection.query.get_or_404(connection_id)
        oracle_service = OracleService(connection)
        
        query = (request.json or {}).get('query')
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        
        analysis = oracle_service.analyze_query(query)
        return jsonify(analysis)
    except Exception as e:
        logger.error(f"Error analyzing query: {str(e)}")
        return jsonify({'error': str(e)}), 500

@oracle_bp.route('/connections/<int:connection_id>/query/execute', methods=['POST'])
def execute_query(connection_id):
    """Execute SQL query and return sample results"""
    try:
        connection = OracleConnection.query.get_or_404(connection_id)
        oracle_service = OracleService(connection)
        
        data = request.json or {}
        query = data.get('query')
        limit = data.get('limit', 10)
        
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        
        results = oracle_service.execute_query(query, limit)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return jsonify({'error': str(e)}), 500
