import logging
from services.oracle_service import OracleService
from services.elasticsearch_service import ElasticsearchService

logger = logging.getLogger(__name__)

class MappingService:
    def __init__(self, oracle_connection, elasticsearch_connection):
        self.oracle_service = OracleService(oracle_connection)
        self.elasticsearch_service = ElasticsearchService(elasticsearch_connection)
    
    def generate_auto_mapping(self, oracle_query, elasticsearch_index):
        """Generate automatic field mapping suggestions"""
        try:
            # Analyze Oracle query to get source fields
            oracle_analysis = self.oracle_service.analyze_query(oracle_query)
            oracle_columns = oracle_analysis['columns']
            
            # Get Elasticsearch index fields if index exists
            es_fields = []
            try:
                es_fields = self.elasticsearch_service.get_index_fields(elasticsearch_index)
            except:
                logger.info(f"Index {elasticsearch_index} doesn't exist yet")
            
            # Generate mapping suggestions
            suggestions = self._generate_mapping_suggestions(oracle_columns, es_fields)
            
            # Generate Elasticsearch mapping
            es_mapping = self._generate_elasticsearch_mapping(oracle_columns)
            
            # Generate transformation rules
            transformation_rules = self._generate_transformation_rules(oracle_columns)
            
            return {
                'source_schema': oracle_columns,
                'suggested_mappings': suggestions,
                'elasticsearch_mapping': es_mapping,
                'transformation_rules': transformation_rules,
                'join_information': oracle_analysis.get('joins', [])
            }
        except Exception as e:
            logger.error(f"Error generating auto mapping: {str(e)}")
            raise
    
    def _generate_mapping_suggestions(self, oracle_columns, es_fields):
        """Generate mapping suggestions between Oracle and Elasticsearch fields"""
        suggestions = []
        
        # Create a mapping of ES field names (lowercased) for easier matching
        es_field_map = {field['field_name'].lower(): field for field in es_fields}
        
        for oracle_col in oracle_columns:
            oracle_field_name = oracle_col['field'].lower()
            
            suggestion = {
                'oracle_field': oracle_col['field'],
                'oracle_type': oracle_col['oracle_type'],
                'suggested_es_field': None,
                'suggested_es_type': oracle_col['elasticsearch_type'],
                'confidence': 0,
                'mapping_type': 'new'  # new, exact_match, similar_match, type_mismatch
            }
            
            # Look for exact match
            if oracle_field_name in es_field_map:
                es_field = es_field_map[oracle_field_name]
                suggestion.update({
                    'suggested_es_field': es_field['field_name'],
                    'suggested_es_type': es_field['type'],
                    'confidence': 100,
                    'mapping_type': 'exact_match'
                })
            else:
                # Look for similar field names
                best_match = self._find_similar_field(oracle_field_name, es_field_map.keys())
                if best_match:
                    es_field = es_field_map[best_match]
                    suggestion.update({
                        'suggested_es_field': es_field['field_name'],
                        'suggested_es_type': es_field['type'],
                        'confidence': 75,
                        'mapping_type': 'similar_match'
                    })
                else:
                    # Suggest new field name based on naming conventions
                    suggested_name = self._suggest_es_field_name(oracle_field_name)
                    suggestion.update({
                        'suggested_es_field': suggested_name,
                        'confidence': 50,
                        'mapping_type': 'new'
                    })
            
            # Check type compatibility
            if (suggestion['suggested_es_field'] and 
                suggestion['mapping_type'] in ['exact_match', 'similar_match']):
                if not self._are_types_compatible(oracle_col['oracle_type'], 
                                               suggestion['suggested_es_type']):
                    suggestion['mapping_type'] = 'type_mismatch'
                    suggestion['confidence'] = max(25, suggestion['confidence'] - 50)
            
            suggestions.append(suggestion)
        
        return suggestions
    
    def _find_similar_field(self, oracle_field, es_field_names):
        """Find similar field names using simple string matching"""
        oracle_field = oracle_field.lower()
        
        # Check for partial matches
        for es_field in es_field_names:
            es_field_lower = es_field.lower()
            
            # Check if Oracle field is contained in ES field or vice versa
            if (oracle_field in es_field_lower or es_field_lower in oracle_field or
                oracle_field.replace('_', '') == es_field_lower.replace('_', '') or
                oracle_field.replace('_', '.') == es_field_lower):
                return es_field
        
        return None
    
    def _suggest_es_field_name(self, oracle_field):
        """Suggest Elasticsearch field name based on naming conventions"""
        # Convert common Oracle naming patterns to ES conventions
        suggestions = {
            'customer_id': 'customer.id',
            'order_id': 'order.id',
            'product_id': 'product.id',
            'user_id': 'user.id',
            'created_date': 'created_at',
            'updated_date': 'updated_at',
            'first_name': 'name.first',
            'last_name': 'name.last',
            'full_name': 'name.full',
            'email_address': 'contact.email',
            'phone_number': 'contact.phone',
            'street_address': 'address.street',
            'zip_code': 'address.postal_code'
        }
        
        oracle_field_lower = oracle_field.lower()
        
        # Check direct mappings first
        if oracle_field_lower in suggestions:
            return suggestions[oracle_field_lower]
        
        # Apply general transformation rules
        suggested_name = oracle_field_lower
        
        # Convert snake_case to dot notation for certain patterns
        if '_id' in suggested_name:
            suggested_name = suggested_name.replace('_id', '.id')
        elif '_date' in suggested_name:
            suggested_name = suggested_name.replace('_date', '_at')
        elif '_name' in suggested_name:
            suggested_name = suggested_name.replace('_name', '.name')
        
        return suggested_name
    
    def _are_types_compatible(self, oracle_type, es_type):
        """Check if Oracle and Elasticsearch types are compatible"""
        compatible_mappings = {
            'NUMBER': ['long', 'integer', 'double', 'float'],
            'VARCHAR2': ['text', 'keyword'],
            'CHAR': ['keyword', 'text'],
            'DATE': ['date'],
            'TIMESTAMP': ['date'],
            'CLOB': ['text'],
            'BLOB': ['binary']
        }
        
        oracle_type_clean = oracle_type.split('(')[0]  # Remove size specifications
        return es_type in compatible_mappings.get(oracle_type_clean, [])
    
    def _generate_elasticsearch_mapping(self, oracle_columns):
        """Generate Elasticsearch mapping from Oracle columns"""
        properties = {}
        
        for col in oracle_columns:
            field_name = col['field']
            es_type = col['elasticsearch_type']
            
            # Handle nested field names (with dots)
            if '.' in field_name:
                self._add_nested_field(properties, field_name, es_type)
            else:
                properties[field_name] = {'type': es_type}
                
                # Add format for date fields
                if es_type == 'date':
                    properties[field_name]['format'] = 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis'
                
                # Add analyzer for text fields
                elif es_type == 'text':
                    properties[field_name]['analyzer'] = 'standard'
        
        return {
            'mappings': {
                'properties': properties
            }
        }
    
    def _add_nested_field(self, properties, field_path, field_type):
        """Add nested field to properties structure"""
        parts = field_path.split('.')
        current = properties
        
        for i, part in enumerate(parts):
            if i == len(parts) - 1:  # Last part - add the actual field
                current[part] = {'type': field_type}
                if field_type == 'date':
                    current[part]['format'] = 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis'
                elif field_type == 'text':
                    current[part]['analyzer'] = 'standard'
            else:  # Intermediate part - ensure object structure
                if part not in current:
                    current[part] = {'type': 'object', 'properties': {}}
                elif 'properties' not in current[part]:
                    current[part]['properties'] = {}
                current = current[part]['properties']
    
    def _generate_transformation_rules(self, oracle_columns):
        """Generate transformation rules for data migration"""
        transformation_rules = []
        
        for col in oracle_columns:
            oracle_field = col['field']
            oracle_type = col['oracle_type']
            es_type = col['elasticsearch_type']
            
            # Generate transformation rules based on type differences
            if oracle_type.startswith('DATE') or oracle_type.startswith('TIMESTAMP'):
                transformation_rules.append({
                    'target': oracle_field,
                    'rule': 'FORMAT_DATE',
                    'description': 'Convert Oracle date to ISO format'
                })
            
            elif oracle_type.startswith('NUMBER') and es_type in ['float', 'double']:
                transformation_rules.append({
                    'target': oracle_field,
                    'rule': 'CAST_FLOAT',
                    'description': 'Cast Oracle NUMBER to float'
                })
            
            elif oracle_type in ['VARCHAR2', 'CHAR'] and 'name' in oracle_field.lower():
                transformation_rules.append({
                    'target': oracle_field,
                    'rule': 'TRIM_SPACES',
                    'description': 'Trim leading/trailing spaces'
                })
        
        return transformation_rules
    
    def validate_mappings(self, field_mappings):
        """Validate field mappings and type compatibility"""
        validation_results = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        for mapping in field_mappings:
            oracle_field = mapping.get('oracle_field')
            oracle_type = mapping.get('oracle_type')
            es_field = mapping.get('es_field')
            es_type = mapping.get('es_type')
            
            # Check required fields
            if not oracle_field or not es_field:
                validation_results['errors'].append(
                    f"Missing required fields in mapping: {mapping}"
                )
                validation_results['valid'] = False
                continue
            
            # Check type compatibility
            if oracle_type and es_type:
                if not self._are_types_compatible(oracle_type, es_type):
                    validation_results['warnings'].append(
                        f"Type mismatch: {oracle_field} ({oracle_type}) -> {es_field} ({es_type})"
                    )
            
            # Check field name conventions
            if '.' in es_field and not self._is_valid_nested_field(es_field):
                validation_results['warnings'].append(
                    f"Potentially invalid nested field name: {es_field}"
                )
        
        return validation_results
    
    def _is_valid_nested_field(self, field_name):
        """Check if nested field name follows valid conventions"""
        # Basic validation - can be enhanced
        parts = field_name.split('.')
        return all(part.isalnum() or '_' in part for part in parts)
