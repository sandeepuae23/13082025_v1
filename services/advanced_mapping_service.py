"""
Advanced Field Mapping Service
Handles nested objects, parent-child relationships, and complex field mappings
"""

import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

class MappingType(Enum):
    DIRECT = "direct"
    NESTED = "nested"
    PARENT_CHILD = "parent_child"
    OBJECT = "object"
    FLATTENED = "flattened"

class RelationshipType(Enum):
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"

@dataclass
class FieldMapping:
    oracle_field: str
    es_field: str
    oracle_type: str
    es_type: str
    mapping_type: MappingType = MappingType.DIRECT
    transformation_rules: List[Dict] = field(default_factory=list)
    nested_path: Optional[str] = None
    parent_field: Optional[str] = None
    relationship_type: Optional[RelationshipType] = None
    is_array: bool = False
    validation_rules: List[Dict] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            'oracle_field': self.oracle_field,
            'es_field': self.es_field,
            'oracle_type': self.oracle_type,
            'es_type': self.es_type,
            'mapping_type': self.mapping_type.value,
            'transformation_rules': self.transformation_rules,
            'nested_path': self.nested_path,
            'parent_field': self.parent_field,
            'relationship_type': self.relationship_type.value if self.relationship_type else None,
            'is_array': self.is_array,
            'validation_rules': self.validation_rules
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FieldMapping':
        return cls(
            oracle_field=data['oracle_field'],
            es_field=data['es_field'],
            oracle_type=data['oracle_type'],
            es_type=data['es_type'],
            mapping_type=MappingType(data.get('mapping_type', 'direct')),
            transformation_rules=data.get('transformation_rules', []),
            nested_path=data.get('nested_path'),
            parent_field=data.get('parent_field'),
            relationship_type=RelationshipType(data['relationship_type']) if data.get('relationship_type') else None,
            is_array=data.get('is_array', False),
            validation_rules=data.get('validation_rules', [])
        )

@dataclass
class NestedMapping:
    name: str
    path: str
    fields: List[FieldMapping]
    include_in_parent: bool = False
    dynamic: bool = True
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'path': self.path,
            'fields': [field.to_dict() for field in self.fields],
            'include_in_parent': self.include_in_parent,
            'dynamic': self.dynamic
        }

@dataclass
class ParentChildMapping:
    parent_type: str
    child_type: str
    join_field: str
    parent_fields: List[FieldMapping]
    child_fields: List[FieldMapping]
    relationship_key: str
    
    def to_dict(self) -> Dict:
        return {
            'parent_type': self.parent_type,
            'child_type': self.child_type,
            'join_field': self.join_field,
            'parent_fields': [field.to_dict() for field in self.parent_fields],
            'child_fields': [field.to_dict() for field in self.child_fields],
            'relationship_key': self.relationship_key
        }

class AdvancedMappingService:
    """Service for managing advanced field mappings"""
    
    def __init__(self):
        self.field_mappings: List[FieldMapping] = []
        self.nested_mappings: List[NestedMapping] = []
        self.parent_child_mappings: List[ParentChildMapping] = []
    
    def analyze_oracle_schema(self, oracle_service, query: str) -> Dict:
        """Analyze Oracle schema and suggest mapping strategies"""
        try:
            # Get query metadata
            metadata = oracle_service.analyze_query(query)
            fields = metadata.get('fields', [])
            tables = metadata.get('tables', [])
            joins = metadata.get('joins', [])
            
            # Analyze relationships
            relationship_analysis = self._analyze_relationships(fields, tables, joins)
            
            # Generate mapping suggestions
            mapping_suggestions = self._generate_mapping_suggestions(fields, relationship_analysis)
            
            return {
                'fields': fields,
                'tables': tables,
                'joins': joins,
                'relationships': relationship_analysis,
                'mapping_suggestions': mapping_suggestions,
                'recommended_strategy': self._recommend_mapping_strategy(relationship_analysis)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing Oracle schema: {e}")
            raise
    
    def _analyze_relationships(self, fields: List[Dict], tables: List[str], joins: List[Dict]) -> Dict:
        """Analyze table relationships and suggest mapping strategies"""
        relationships = {
            'one_to_one': [],
            'one_to_many': [],
            'many_to_many': [],
            'nested_candidates': [],
            'parent_child_candidates': []
        }
        
        # Analyze join patterns
        for join in joins:
            join_type = join.get('type', 'INNER').upper()
            left_table = join.get('left_table')
            right_table = join.get('right_table')
            left_field = join.get('left_field')
            right_field = join.get('right_field')
            
            # Determine relationship type based on field names and patterns
            if self._is_primary_key(left_field) and self._is_foreign_key(right_field):
                if self._suggests_one_to_many(left_table, right_table):
                    relationships['one_to_many'].append({
                        'parent_table': left_table,
                        'child_table': right_table,
                        'parent_key': left_field,
                        'foreign_key': right_field,
                        'suggested_mapping': 'nested' if self._suitable_for_nesting(right_table) else 'parent_child'
                    })
                else:
                    relationships['one_to_one'].append({
                        'table1': left_table,
                        'table2': right_table,
                        'key1': left_field,
                        'key2': right_field,
                        'suggested_mapping': 'object'
                    })
        
        # Identify nested candidates based on table names and field patterns
        for table in tables:
            if self._is_detail_table(table):
                master_table = self._find_master_table(table, tables)
                if master_table:
                    relationships['nested_candidates'].append({
                        'parent_table': master_table,
                        'nested_table': table,
                        'suggested_path': self._generate_nested_path(table)
                    })
        
        # Identify parent-child candidates for large hierarchical data
        for table in tables:
            if self._has_hierarchical_structure(table, fields):
                relationships['parent_child_candidates'].append({
                    'table': table,
                    'hierarchy_field': self._find_hierarchy_field(table, fields),
                    'suggested_join_field': 'document_relationship'
                })
        
        return relationships
    
    def _generate_mapping_suggestions(self, fields: List[Dict], relationships: Dict) -> List[Dict]:
        """Generate intelligent mapping suggestions"""
        suggestions = []
        
        # Direct field mappings
        for field in fields:
            oracle_name = field.get('name', '')
            oracle_type = field.get('type', '')
            table_name = field.get('table', '')
            
            suggestion = {
                'oracle_field': f"{table_name}.{oracle_name}",
                'oracle_type': oracle_type,
                'suggested_es_field': self._suggest_es_field_name(oracle_name),
                'suggested_es_type': self._suggest_es_type(oracle_type),
                'mapping_type': 'direct',
                'confidence': self._calculate_mapping_confidence(oracle_name, oracle_type),
                'transformation_suggestions': self._suggest_transformations(oracle_name, oracle_type)
            }
            suggestions.append(suggestion)
        
        # Nested object suggestions
        for nested_candidate in relationships.get('nested_candidates', []):
            parent_table = nested_candidate['parent_table']
            nested_table = nested_candidate['nested_table']
            nested_path = nested_candidate['suggested_path']
            
            # Get fields from nested table
            nested_fields = [f for f in fields if f.get('table') == nested_table]
            
            suggestion = {
                'mapping_type': 'nested',
                'parent_table': parent_table,
                'nested_table': nested_table,
                'nested_path': nested_path,
                'nested_fields': [
                    {
                        'oracle_field': f"{nested_table}.{field.get('name', '')}",
                        'suggested_es_field': f"{nested_path}.{self._suggest_es_field_name(field.get('name', ''))}",
                        'oracle_type': field.get('type', ''),
                        'suggested_es_type': self._suggest_es_type(field.get('type', ''))
                    }
                    for field in nested_fields
                ],
                'confidence': 85
            }
            suggestions.append(suggestion)
        
        # Parent-child suggestions
        for pc_candidate in relationships.get('parent_child_candidates', []):
            suggestion = {
                'mapping_type': 'parent_child',
                'table': pc_candidate['table'],
                'join_field': pc_candidate['suggested_join_field'],
                'hierarchy_field': pc_candidate['hierarchy_field'],
                'confidence': 75
            }
            suggestions.append(suggestion)
        
        return suggestions
    
    def create_nested_mapping(self, parent_table: str, nested_config: Dict) -> NestedMapping:
        """Create a nested object mapping"""
        nested_fields = []
        
        for field_config in nested_config.get('fields', []):
            field_mapping = FieldMapping(
                oracle_field=field_config['oracle_field'],
                es_field=field_config['es_field'],
                oracle_type=field_config['oracle_type'],
                es_type=field_config['es_type'],
                mapping_type=MappingType.NESTED,
                nested_path=nested_config['path'],
                transformation_rules=field_config.get('transformation_rules', [])
            )
            nested_fields.append(field_mapping)
        
        nested_mapping = NestedMapping(
            name=nested_config['name'],
            path=nested_config['path'],
            fields=nested_fields,
            include_in_parent=nested_config.get('include_in_parent', False),
            dynamic=nested_config.get('dynamic', True)
        )
        
        self.nested_mappings.append(nested_mapping)
        return nested_mapping
    
    def create_parent_child_mapping(self, config: Dict) -> ParentChildMapping:
        """Create a parent-child relationship mapping"""
        parent_fields = [
            FieldMapping.from_dict(field_data) 
            for field_data in config.get('parent_fields', [])
        ]
        
        child_fields = [
            FieldMapping.from_dict(field_data) 
            for field_data in config.get('child_fields', [])
        ]
        
        pc_mapping = ParentChildMapping(
            parent_type=config['parent_type'],
            child_type=config['child_type'],
            join_field=config['join_field'],
            parent_fields=parent_fields,
            child_fields=child_fields,
            relationship_key=config['relationship_key']
        )
        
        self.parent_child_mappings.append(pc_mapping)
        return pc_mapping
    
    def generate_elasticsearch_mapping(self) -> Dict:
        """Generate complete Elasticsearch mapping from configured mappings"""
        es_mapping = {
            "mappings": {
                "properties": {}
            }
        }
        
        # Add direct field mappings
        for field_mapping in self.field_mappings:
            if field_mapping.mapping_type == MappingType.DIRECT:
                es_mapping["mappings"]["properties"][field_mapping.es_field] = {
                    "type": field_mapping.es_type
                }
                
                # Add additional properties based on type
                if field_mapping.es_type == "text":
                    es_mapping["mappings"]["properties"][field_mapping.es_field]["fields"] = {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
        
        # Add nested mappings
        for nested_mapping in self.nested_mappings:
            nested_properties = {}
            
            for field in nested_mapping.fields:
                nested_properties[field.es_field.split('.')[-1]] = {
                    "type": field.es_type
                }
            
            es_mapping["mappings"]["properties"][nested_mapping.path] = {
                "type": "nested",
                "dynamic": nested_mapping.dynamic,
                "include_in_parent": nested_mapping.include_in_parent,
                "properties": nested_properties
            }
        
        # Add parent-child mappings
        if self.parent_child_mappings:
            for pc_mapping in self.parent_child_mappings:
                # Add join field for parent-child relationship
                es_mapping["mappings"]["properties"][pc_mapping.join_field] = {
                    "type": "join",
                    "relations": {
                        pc_mapping.parent_type: pc_mapping.child_type
                    }
                }
        
        return es_mapping
    
    def generate_transformation_query(self, oracle_query: str) -> Dict:
        """Generate transformation query for complex mappings"""
        transformation = {
            'base_query': oracle_query,
            'transformations': [],
            'grouping_strategy': self._determine_grouping_strategy(),
            'post_processing': []
        }
        
        # Add nested object transformations
        for nested_mapping in self.nested_mappings:
            transformation['transformations'].append({
                'type': 'nested_grouping',
                'parent_key': self._find_parent_key(nested_mapping),
                'nested_path': nested_mapping.path,
                'fields': [field.oracle_field for field in nested_mapping.fields]
            })
        
        # Add parent-child transformations
        for pc_mapping in self.parent_child_mappings:
            transformation['transformations'].append({
                'type': 'parent_child_split',
                'parent_type': pc_mapping.parent_type,
                'child_type': pc_mapping.child_type,
                'relationship_key': pc_mapping.relationship_key,
                'parent_fields': [field.oracle_field for field in pc_mapping.parent_fields],
                'child_fields': [field.oracle_field for field in pc_mapping.child_fields]
            })
        
        return transformation
    
    # Helper methods for analysis and suggestions
    
    def _is_primary_key(self, field_name: str) -> bool:
        """Check if field name suggests it's a primary key"""
        pk_patterns = ['_id', 'id', '_key', 'key', '_pk']
        return any(pattern in field_name.lower() for pattern in pk_patterns)
    
    def _is_foreign_key(self, field_name: str) -> bool:
        """Check if field name suggests it's a foreign key"""
        fk_patterns = ['_id', '_ref', '_fk', 'ref_']
        return any(pattern in field_name.lower() for pattern in fk_patterns)
    
    def _suggests_one_to_many(self, table1: str, table2: str) -> bool:
        """Check if table names suggest one-to-many relationship"""
        detail_patterns = ['detail', 'item', 'line', 'entry']
        return any(pattern in table2.lower() for pattern in detail_patterns)
    
    def _suitable_for_nesting(self, table_name: str) -> bool:
        """Check if table is suitable for nested mapping"""
        # Tables with fewer expected records are better for nesting
        small_table_patterns = ['detail', 'item', 'attribute', 'property']
        return any(pattern in table_name.lower() for pattern in small_table_patterns)
    
    def _is_detail_table(self, table_name: str) -> bool:
        """Check if table is a detail/child table"""
        detail_patterns = ['detail', 'item', 'line', 'entry', 'attr']
        return any(pattern in table_name.lower() for pattern in detail_patterns)
    
    def _find_master_table(self, detail_table: str, all_tables: List[str]) -> Optional[str]:
        """Find the master table for a detail table"""
        detail_base = detail_table.lower().replace('_detail', '').replace('_item', '').replace('_line', '')
        
        for table in all_tables:
            if table.lower() == detail_base or detail_base in table.lower():
                return table
        return None
    
    def _generate_nested_path(self, table_name: str) -> str:
        """Generate nested path name from table name"""
        # Convert ORDER_ITEMS to order_items, then to items
        clean_name = table_name.lower()
        if '_' in clean_name:
            parts = clean_name.split('_')
            return parts[-1]  # Use the last part (e.g., 'items' from 'order_items')
        return clean_name
    
    def _has_hierarchical_structure(self, table_name: str, fields: List[Dict]) -> bool:
        """Check if table has hierarchical structure"""
        table_fields = [f for f in fields if f.get('table') == table_name]
        field_names = [f.get('name', '').lower() for f in table_fields]
        
        hierarchy_patterns = ['parent_id', 'manager_id', 'superior_id', 'level']
        return any(pattern in field_names for pattern in hierarchy_patterns)
    
    def _find_hierarchy_field(self, table_name: str, fields: List[Dict]) -> Optional[str]:
        """Find the field that defines hierarchy"""
        table_fields = [f for f in fields if f.get('table') == table_name]
        
        for field in table_fields:
            field_name = field.get('name', '').lower()
            if 'parent_id' in field_name or 'manager_id' in field_name:
                return field.get('name')
        return None
    
    def _suggest_es_field_name(self, oracle_field: str) -> str:
        """Suggest Elasticsearch field name from Oracle field name"""
        # Convert Oracle naming conventions to ES conventions
        es_name = oracle_field.lower()
        
        # Common conversions
        conversions = {
            '_id': '_id',
            '_date': '_date',
            '_time': '_time',
            '_amount': '_amount',
            '_price': '_price',
            '_qty': '_quantity',
            '_desc': '_description',
            '_addr': '_address'
        }
        
        for oracle_suffix, es_suffix in conversions.items():
            if es_name.endswith(oracle_suffix):
                es_name = es_name.replace(oracle_suffix, es_suffix)
                break
        
        return es_name
    
    def _suggest_es_type(self, oracle_type: str) -> str:
        """Suggest Elasticsearch type from Oracle type"""
        oracle_type = oracle_type.upper()
        
        type_mappings = {
            'VARCHAR2': 'text',
            'VARCHAR': 'text',
            'CHAR': 'keyword',
            'NUMBER': 'long',
            'INTEGER': 'integer',
            'FLOAT': 'float',
            'DATE': 'date',
            'TIMESTAMP': 'date',
            'CLOB': 'text',
            'BLOB': 'binary',
            'RAW': 'binary'
        }
        
        # Handle NUMBER with precision
        if oracle_type.startswith('NUMBER'):
            if ',' in oracle_type:  # Has decimal places
                return 'scaled_float'
            else:
                return 'long'
        
        # Handle VARCHAR2 with length
        if oracle_type.startswith('VARCHAR2') or oracle_type.startswith('VARCHAR'):
            # Extract length if available
            if '(' in oracle_type:
                try:
                    length_str = oracle_type.split('(')[1].split(')')[0]
                    length = int(length_str)
                    if length <= 256:
                        return 'keyword'
                    else:
                        return 'text'
                except:
                    pass
            return 'text'
        
        return type_mappings.get(oracle_type, 'text')
    
    def _calculate_mapping_confidence(self, field_name: str, oracle_type: str) -> int:
        """Calculate confidence score for mapping suggestion"""
        confidence = 70  # Base confidence
        
        # Boost confidence for clear patterns
        if '_id' in field_name.lower() and 'NUMBER' in oracle_type.upper():
            confidence += 20
        
        if '_date' in field_name.lower() and 'DATE' in oracle_type.upper():
            confidence += 25
        
        if '_amount' in field_name.lower() and 'NUMBER' in oracle_type.upper():
            confidence += 15
        
        # Reduce confidence for complex types
        if 'CLOB' in oracle_type.upper() or 'BLOB' in oracle_type.upper():
            confidence -= 10
        
        return min(confidence, 95)
    
    def _suggest_transformations(self, field_name: str, oracle_type: str) -> List[Dict]:
        """Suggest transformation rules for field"""
        transformations = []
        
        # Date transformations
        if 'DATE' in oracle_type.upper() or 'TIMESTAMP' in oracle_type.upper():
            transformations.append({
                'type': 'date_format',
                'description': 'Convert to ISO 8601 format',
                'config': {
                    'from_format': 'YYYY-MM-DD HH24:MI:SS',
                    'to_format': 'iso8601'
                }
            })
        
        # String transformations
        if 'VARCHAR' in oracle_type.upper():
            transformations.append({
                'type': 'string_cleanup',
                'description': 'Trim whitespace and normalize',
                'config': {
                    'trim': True,
                    'normalize_unicode': True
                }
            })
        
        # Numeric transformations
        if 'NUMBER' in oracle_type.upper() and ',' in oracle_type:
            transformations.append({
                'type': 'numeric_scaling',
                'description': 'Scale decimal values for precision',
                'config': {
                    'scale_factor': 100
                }
            })
        
        return transformations
    
    def _recommend_mapping_strategy(self, relationships: Dict) -> Dict:
        """Recommend overall mapping strategy"""
        strategy = {
            'primary_approach': 'direct',
            'use_nested': False,
            'use_parent_child': False,
            'reasoning': []
        }
        
        # Check for nested candidates
        if relationships.get('nested_candidates'):
            strategy['use_nested'] = True
            strategy['primary_approach'] = 'hybrid'
            strategy['reasoning'].append('Detected suitable tables for nested object mapping')
        
        # Check for parent-child candidates
        if relationships.get('parent_child_candidates'):
            strategy['use_parent_child'] = True
            strategy['primary_approach'] = 'hybrid'
            strategy['reasoning'].append('Detected hierarchical data suitable for parent-child mapping')
        
        # Check for many-to-many relationships
        if relationships.get('many_to_many'):
            strategy['reasoning'].append('Complex many-to-many relationships may require denormalization')
        
        return strategy
    
    def _determine_grouping_strategy(self) -> str:
        """Determine how to group data during transformation"""
        if self.nested_mappings:
            return 'nested_grouping'
        elif self.parent_child_mappings:
            return 'parent_child_grouping'
        else:
            return 'direct_mapping'
    
    def _find_parent_key(self, nested_mapping: NestedMapping) -> str:
        """Find the parent key for nested mapping"""
        # This would typically be determined from the relationship analysis
        return 'id'  # Default assumption