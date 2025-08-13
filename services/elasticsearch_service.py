from elasticsearch import Elasticsearch
import logging

logger = logging.getLogger(__name__)

class ElasticsearchService:
    def __init__(self, connection_config):
        self.config = connection_config
        self.client = None
    
    def get_client(self):
        """Get Elasticsearch client"""
        if not self.client:
            try:
                # Build connection URL
                protocol = 'https' if self.config.use_ssl else 'http'
                url = f"{protocol}://{self.config.host}:{self.config.port}"
                
                # Setup authentication if provided
                auth = None
                if self.config.username and self.config.password:
                    auth = (self.config.username, self.config.password)
                
                self.client = Elasticsearch(
                    [url],
                    http_auth=auth,
                    verify_certs=self.config.use_ssl,
                    connection_class=None
                )
            except Exception as e:
                logger.error(f"Failed to connect to Elasticsearch: {str(e)}")
                raise
        return self.client
    
    def test_connection(self):
        """Test Elasticsearch connection"""
        try:
            client = self.get_client()
            info = client.info()
            return info.get('cluster_name') is not None
        except Exception as e:
            logger.error(f"Elasticsearch connection test failed: {str(e)}")
            return False
    
    def get_indices(self):
        """Get all indices from Elasticsearch cluster"""
        try:
            client = self.get_client()
            
            # Get indices with stats
            indices_response = client.cat.indices(format='json', h='index,docs.count,store.size')
            
            indices = []
            for index in indices_response:
                # Skip system indices that start with '.'
                if not index['index'].startswith('.'):
                    indices.append({
                        'index_name': index['index'],
                        'doc_count': int(index['docs.count']) if index['docs.count'] else 0,
                        'store_size': index['store.size'] if index['store.size'] else '0b'
                    })
            
            return sorted(indices, key=lambda x: x['index_name'])
        except Exception as e:
            logger.error(f"Error fetching Elasticsearch indices: {str(e)}")
            raise
    
    def get_index_mapping(self, index_name):
        """Get mapping for a specific index"""
        try:
            client = self.get_client()
            mapping = client.indices.get_mapping(index=index_name)
            return mapping[index_name]['mappings']
        except Exception as e:
            logger.error(f"Error fetching index mapping: {str(e)}")
            raise
    
    def get_index_fields(self, index_name):
        """Get all fields from an Elasticsearch index"""
        try:
            mapping = self.get_index_mapping(index_name)
            fields = []
            
            def extract_fields(properties, prefix=''):
                for field_name, field_config in properties.items():
                    full_name = f"{prefix}.{field_name}" if prefix else field_name
                    
                    field_info = {
                        'field_name': full_name,
                        'type': field_config.get('type', 'object'),
                        'format': field_config.get('format'),
                        'analyzer': field_config.get('analyzer')
                    }
                    fields.append(field_info)
                    
                    # Handle nested objects
                    if 'properties' in field_config:
                        extract_fields(field_config['properties'], full_name)
            
            if 'properties' in mapping:
                extract_fields(mapping['properties'])
            
            return sorted(fields, key=lambda x: x['field_name'])
        except Exception as e:
            logger.error(f"Error fetching index fields: {str(e)}")
            raise
    
    def create_index(self, index_name, mapping=None):
        """Create a new Elasticsearch index"""
        try:
            client = self.get_client()
            
            body = {}
            if mapping:
                body['mappings'] = mapping
            
            response = client.indices.create(index=index_name, body=body)
            return {'success': True, 'acknowledged': response.get('acknowledged', False)}
        except Exception as e:
            logger.error(f"Error creating index: {str(e)}")
            raise
    
    def index_document(self, index_name, document, doc_id=None):
        """Index a single document"""
        try:
            client = self.get_client()
            
            if doc_id:
                response = client.index(index=index_name, id=doc_id, body=document)
            else:
                response = client.index(index=index_name, body=document)
            
            return response
        except Exception as e:
            logger.error(f"Error indexing document: {str(e)}")
            raise
    
    def bulk_index(self, index_name, documents):
        """Bulk index multiple documents"""
        try:
            client = self.get_client()
            
            # Prepare bulk actions
            actions = []
            for doc in documents:
                action = {
                    '_index': index_name,
                    '_source': doc
                }
                actions.append(action)
            
            # Execute bulk request
            response = client.bulk(body=actions)
            
            # Count successful and failed operations
            success_count = 0
            failed_count = 0
            errors = []
            
            for item in response['items']:
                if 'index' in item:
                    if item['index']['status'] in [200, 201]:
                        success_count += 1
                    else:
                        failed_count += 1
                        errors.append(item['index'].get('error', 'Unknown error'))
            
            return {
                'success_count': success_count,
                'failed_count': failed_count,
                'errors': errors[:10]  # Limit to first 10 errors
            }
        except Exception as e:
            logger.error(f"Error bulk indexing: {str(e)}")
            raise
    
    def delete_index(self, index_name):
        """Delete an Elasticsearch index"""
        try:
            client = self.get_client()
            response = client.indices.delete(index=index_name)
            return {'success': True, 'acknowledged': response.get('acknowledged', False)}
        except Exception as e:
            logger.error(f"Error deleting index: {str(e)}")
            raise
    
    def get_cluster_health(self):
        """Get Elasticsearch cluster health"""
        try:
            client = self.get_client()
            health = client.cluster.health()
            return health
        except Exception as e:
            logger.error(f"Error fetching cluster health: {str(e)}")
            raise
