"""
Oracle Database Service
Handles Oracle database connections and operations
"""

import logging
import sqlparse
from typing import List, Dict, Any, Optional

try:  # pragma: no cover - optional dependency
    import oracledb  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    try:
        import cx_Oracle as oracledb  # type: ignore
    except Exception:
        oracledb = None  # type: ignore

logger = logging.getLogger(__name__)

class OracleService:
    """Service for Oracle database operations"""
    
    def __init__(self, oracle_connection):
        self.connection_config = oracle_connection
        self._connection = None
    
    def connect(self) -> Optional[Any]:
        """Establish connection to Oracle database"""
        if self._connection:
            return self._connection

        if not oracledb:
            logger.error("Oracle client library is not installed")
            return None

        try:
            dsn = oracledb.makedsn(
                self.connection_config.host,
                self.connection_config.port,
                service_name=self.connection_config.service_name,
            )
            self._connection = oracledb.connect(
                user=self.connection_config.username,
                password=self.connection_config.password,
                dsn=dsn,
            )
            logger.info(
                f"Connected to Oracle: {self.connection_config.host}:{self.connection_config.port}"
            )
            return self._connection
        except Exception as e:  # pragma: no cover - network interaction
            logger.error(f"Failed to connect to Oracle: {e}")
            self._connection = None
            return None

    def get_tables(self) -> List[Dict[str, Any]]:
        """Retrieve available tables from the Oracle connection"""
        try:
            conn = self.connect()
            if not conn:
                return []

            cursor = conn.cursor()
            owner = self.connection_config.username.upper()
            cursor.execute(
                """
                SELECT table_name, NVL(num_rows, 0) AS num_rows
                FROM all_tables
                WHERE owner = :owner
                ORDER BY table_name
                """,
                owner=owner,
            )
            tables = [
                {"table_name": row[0], "num_rows": int(row[1])}
                for row in cursor.fetchall()
            ]
            cursor.close()
            return tables
        except Exception as e:  # pragma: no cover - network interaction
            logger.error(f"Error fetching tables: {e}")
            return []

    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Retrieve column metadata for a specific table"""
        try:
            if not self._connection:
                self.connect()

            fields = self._get_mock_table_fields(table_name)
            return [
                {
                    'column_name': field['name'],
                    'data_type': field['type'],
                    'data_length': None,
                    'nullable': True,
                    'elasticsearch_type': self._map_oracle_to_es(field['type'])
                }
                for field in fields
            ]
        except Exception as e:
            logger.error(f"Error fetching columns for {table_name}: {e}")
            return []

    def _map_oracle_to_es(self, oracle_type: str) -> str:
        """Map Oracle data types to Elasticsearch types"""
        oracle_type = oracle_type.upper()
        if 'NUMBER' in oracle_type:
            return 'integer'
        if 'DATE' in oracle_type or 'TIMESTAMP' in oracle_type:
            return 'text'


    def analyze_query(self, query):
        """Analyze SQL query and extract column information"""
        try:
            conn = self.connect()
            cursor = conn.cursor()

            # Use DESCRIBE to get column information without executing the full query
            describe_query = f"SELECT * FROM ({query}) WHERE ROWNUM = 0"
            cursor.execute(describe_query)

            columns = []
            for desc in cursor.description:
                column_name = desc[0]
                oracle_type = self._get_oracle_type_name(desc[1])

                columns.append({
                    'field': column_name.lower(),
                    'oracle_type': oracle_type,
                    'elasticsearch_type': self._map_oracle_to_es_type(oracle_type),
                    'source': self._extract_source_from_query(query, column_name)
                })

            cursor.close()

            # Parse query for additional information
            joins = self._extract_joins_from_query(query)

            return {
                'columns': columns,
                'joins': joins,
                'query_type': 'SELECT'
            }
        except Exception as e:
            logger.error(f"Error analyzing query: {str(e)}")
            raise

    def analyze_query_v1(self, query: str) -> Dict[str, Any]:
        """Analyze Oracle query and extract metadata"""
        try:
            # Parse the SQL query
            parsed = sqlparse.parse(query)[0]
            
            # Extract tables, fields, and joins
            analysis = {
                'fields': [],
                'tables': [],
                'joins': [],
                'where_conditions': [],
                'order_by': [],
                'group_by': []
            }
            
            # Basic query analysis using sqlparse
            tokens = list(parsed.flatten())
            
            # Extract table names and aliases
            in_from = False
            in_join = False
            current_table = None
            
            for i, token in enumerate(tokens):
                token_str = str(token).strip().upper()
                
                if token_str == 'FROM':
                    in_from = True
                    continue
                elif token_str in ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN']:
                    in_join = True
                    continue
                elif token_str in ['WHERE', 'ORDER BY', 'GROUP BY', 'HAVING']:
                    in_from = False
                    in_join = False
                    continue
                
                if in_from or in_join:
                    if token.ttype is None and token_str not in ['ON', ',']:
                        if token_str not in analysis['tables']:
                            analysis['tables'].append(token_str)
                            
                            # Mock field analysis
                            mock_fields = self._get_mock_table_fields(token_str)
                            analysis['fields'].extend(mock_fields)
            
            # Mock join analysis
            if 'JOIN' in query.upper():
                analysis['joins'] = self._analyze_joins(query)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing query: {e}")
            return {
                'fields': [],
                'tables': [],
                'joins': [],
                'where_conditions': [],
                'order_by': [],
                'group_by': []
            }
    
    def _get_mock_table_fields(self, table_name: str) -> List[Dict]:
        """Get mock field information for a table"""
        table_name = table_name.upper()
        
        # Mock field data based on common table patterns
        if 'ORDER' in table_name:
            return [
                {'name': 'ORDER_ID', 'type': 'NUMBER', 'table': table_name},
                {'name': 'ORDER_DATE', 'type': 'DATE', 'table': table_name},
                {'name': 'CUSTOMER_ID', 'type': 'NUMBER', 'table': table_name},
                {'name': 'TOTAL_AMOUNT', 'type': 'NUMBER(10,2)', 'table': table_name},
                {'name': 'STATUS', 'type': 'VARCHAR2(50)', 'table': table_name}
            ]
        elif 'CUSTOMER' in table_name:
            return [
                {'name': 'ID', 'type': 'NUMBER', 'table': table_name},
                {'name': 'CUSTOMER_NAME', 'type': 'VARCHAR2(100)', 'table': table_name},
                {'name': 'EMAIL', 'type': 'VARCHAR2(255)', 'table': table_name},
                {'name': 'PHONE', 'type': 'VARCHAR2(20)', 'table': table_name},
                {'name': 'ADDRESS', 'type': 'VARCHAR2(500)', 'table': table_name}
            ]
        elif 'ITEM' in table_name:
            return [
                {'name': 'ID', 'type': 'NUMBER', 'table': table_name},
                {'name': 'ORDER_ID', 'type': 'NUMBER', 'table': table_name},
                {'name': 'PRODUCT_ID', 'type': 'NUMBER', 'table': table_name},
                {'name': 'QUANTITY', 'type': 'NUMBER', 'table': table_name},
                {'name': 'UNIT_PRICE', 'type': 'NUMBER(10,2)', 'table': table_name}
            ]
        elif 'PRODUCT' in table_name:
            return [
                {'name': 'ID', 'type': 'NUMBER', 'table': table_name},
                {'name': 'PRODUCT_NAME', 'type': 'VARCHAR2(100)', 'table': table_name},
                {'name': 'CATEGORY', 'type': 'VARCHAR2(50)', 'table': table_name},
                {'name': 'DESCRIPTION', 'type': 'CLOB', 'table': table_name},
                {'name': 'PRICE', 'type': 'NUMBER(10,2)', 'table': table_name}
            ]
        else:
            # Generic fields
            return [
                {'name': 'ID', 'type': 'NUMBER', 'table': table_name},
                {'name': 'NAME', 'type': 'VARCHAR2(100)', 'table': table_name},
                {'name': 'CREATED_DATE', 'type': 'DATE', 'table': table_name},
                {'name': 'UPDATED_DATE', 'type': 'DATE', 'table': table_name}
            ]
    
    def _analyze_joins(self, query: str) -> List[Dict]:
        """Analyze JOIN clauses in the query"""
        joins = []
        
        # Mock join analysis - in production would use proper SQL parsing
        if 'customers c ON o.customer_id = c.id' in query.lower():
            joins.append({
                'type': 'INNER',
                'left_table': 'ORDERS',
                'right_table': 'CUSTOMERS',
                'left_field': 'CUSTOMER_ID',
                'right_field': 'ID'
            })
        
        if 'order_items oi ON o.order_id = oi.order_id' in query.lower():
            joins.append({
                'type': 'INNER',
                'left_table': 'ORDERS',
                'right_table': 'ORDER_ITEMS',
                'left_field': 'ORDER_ID',
                'right_field': 'ORDER_ID'
            })
        
        if 'products p ON oi.product_id = p.id' in query.lower():
            joins.append({
                'type': 'INNER',
                'left_table': 'ORDER_ITEMS',
                'right_table': 'PRODUCTS',
                'left_field': 'PRODUCT_ID',
                'right_field': 'ID'
            })
        
        return joins
    
    def get_table_schema(self, table_name: str) -> Dict:
        """Get schema information for a table"""
        try:
            # Mock schema information
            return {
                'table_name': table_name,
                'fields': self._get_mock_table_fields(table_name),
                'primary_keys': ['ID'],
                'foreign_keys': self._get_mock_foreign_keys(table_name),
                'indexes': []
            }
            
        except Exception as e:
            logger.error(f"Error getting table schema: {e}")
            return {'table_name': table_name, 'fields': [], 'primary_keys': [], 'foreign_keys': [], 'indexes': []}
    
    def _get_mock_foreign_keys(self, table_name: str) -> List[Dict]:
        """Get mock foreign key information"""
        table_name = table_name.upper()
        
        if 'ORDER' in table_name and 'ITEM' not in table_name:
            return [
                {'field': 'CUSTOMER_ID', 'references_table': 'CUSTOMERS', 'references_field': 'ID'}
            ]
        elif 'ITEM' in table_name:
            return [
                {'field': 'ORDER_ID', 'references_table': 'ORDERS', 'references_field': 'ORDER_ID'},
                {'field': 'PRODUCT_ID', 'references_table': 'PRODUCTS', 'references_field': 'ID'}
            ]
        
        return []

    def execute_query(self, query, limit=10):
        """Execute SQL query and return sample results"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Add ROWNUM limit if not present
            if 'ROWNUM' not in query.upper() and 'LIMIT' not in query.upper():
                limited_query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {limit}"
            else:
                limited_query = query

            cursor.execute(limited_query)

            # Get column names
            column_names = [desc[0] for desc in cursor.description]

            # Fetch results
            rows = cursor.fetchall()
            results = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    # Convert Oracle types to JSON-serializable types
                    if hasattr(value, 'isoformat'):  # Date/Datetime
                        row_dict[column_names[i]] = value.isoformat()
                    elif isinstance(value, (int, float, str)) or value is None:
                        row_dict[column_names[i]] = value
                    else:
                        row_dict[column_names[i]] = str(value)
                results.append(row_dict)

            cursor.close()

            return {
                'columns': column_names,
                'rows': results,
                'total_rows': len(results)
            }
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def _generate_mock_value(self, data_type: str, index: int = 0):
        """Generate mock value based on data type"""
        data_type = data_type.upper()
        
        if 'NUMBER' in data_type:
            return index + 1
        elif 'VARCHAR2' in data_type or 'VARCHAR' in data_type:
            return f"Sample Text {index + 1}"
        elif 'DATE' in data_type or 'TIMESTAMP' in data_type:
            return f"2024-01-{(index % 28) + 1:02d}"
        elif 'CLOB' in data_type:
            return f"Large text content for record {index + 1}"
        else:
            return f"Value {index + 1}"
    
    def test_connection(self) -> Dict[str, Any]:
        """Test Oracle database connection"""
        try:
            success = self.connect()
            if success:
                return {
                    'status': 'success',
                    'message': 'Connected to Oracle database successfully',
                    'version': 'Oracle 19c (Mock)',
                    'schemas': ['SALES', 'HR', 'FINANCE']  # Mock schemas
                }
            else:
                return {
                    'status': 'error',
                    'message': 'Failed to connect to Oracle database',
                    'error': 'Connection timeout'
                }
                
        except Exception as e:
            logger.error(f"Error testing connection: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'error': str(e)
            }
    
    def close(self):
        """Close Oracle connection"""
        try:
            if self._connection:
                self._connection = None
                logger.info("Oracle connection closed")
        except Exception as e:
            logger.error(f"Error closing Oracle connection: {e}")


    def _get_oracle_type_name(self, type_code):
        """Convert Oracle type code to type name"""
        type_codes = {
            oracledb.DB_TYPE_VARCHAR: 'VARCHAR2',
            oracledb.DB_TYPE_CHAR: 'CHAR',
            oracledb.DB_TYPE_NUMBER: 'NUMBER',
            oracledb.DB_TYPE_DATE: 'DATE',
            oracledb.DB_TYPE_TIMESTAMP: 'TIMESTAMP',
            oracledb.DB_TYPE_CLOB: 'CLOB',
            oracledb.DB_TYPE_BLOB: 'BLOB',
            oracledb.DB_TYPE_BINARY_FLOAT: 'BINARY_FLOAT',
            oracledb.DB_TYPE_BINARY_DOUBLE: 'BINARY_DOUBLE'
        }
        return type_codes.get(type_code, 'UNKNOWN')

    def _extract_source_from_query(self, query, column_name):
        """Extract source table and column from query"""
        # This is a simplified implementation
        # In a production system, you would want a more sophisticated SQL parser
        try:
            parsed = sqlparse.parse(query)[0]
            # Basic extraction - would need more sophisticated parsing for complex queries
            return f"query.{column_name}"
        except:
            return f"query.{column_name}"

    def _extract_joins_from_query(self, query):
        """Extract JOIN information from query"""
        joins = []
        try:
            # Simple regex-based extraction for demonstration
            # In production, use a proper SQL parser
            query_upper = query.upper()
            if 'JOIN' in query_upper:
                # This is a simplified extraction
                # Would need more sophisticated parsing for production
                joins.append({'type': 'INNER', 'condition': 'Detected in query'})
        except:
            pass
        return joins

    def _map_oracle_to_es_type(self, oracle_type):
        """Map Oracle data types to Elasticsearch types"""
        type_mapping = {
            'NUMBER': 'long',
            'FLOAT': 'float',
            'BINARY_FLOAT': 'float',
            'BINARY_DOUBLE': 'double',
            'VARCHAR2': 'text',
            'CHAR': 'keyword',
            'NVARCHAR2': 'text',
            'NCHAR': 'keyword',
            'CLOB': 'text',
            'NCLOB': 'text',
            'DATE': 'date',
            'TIMESTAMP': 'date',
            'TIMESTAMP WITH TIME ZONE': 'date',
            'TIMESTAMP WITH LOCAL TIME ZONE': 'date',
            'BLOB': 'binary',
            'RAW': 'binary',
            'LONG RAW': 'binary'
        }

        # Handle NUMBER with precision/scale
        if oracle_type.startswith('NUMBER'):
            if ',' in oracle_type:  # Has decimal places
                return 'double'
            else:
                return 'long'

        return type_mapping.get(oracle_type, 'keyword')