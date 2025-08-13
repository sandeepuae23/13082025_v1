from app import db
from datetime import datetime
import json

class OracleConnection(db.Model):
    __tablename__ = 'oracle_connections'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    host = db.Column(db.String(255), nullable=False)
    port = db.Column(db.Integer, nullable=False, default=1521)
    service_name = db.Column(db.String(100), nullable=False)
    username = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(255), nullable=False)  # Should be encrypted in production
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)

class ElasticsearchConnection(db.Model):
    __tablename__ = 'elasticsearch_connections'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    environment = db.Column(db.String(50), nullable=False)  # dev, staging, prod
    host = db.Column(db.String(255), nullable=False)
    port = db.Column(db.Integer, nullable=False, default=9200)
    username = db.Column(db.String(100))
    password = db.Column(db.String(255))  # Should be encrypted in production
    use_ssl = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)

class MappingConfiguration(db.Model):
    __tablename__ = 'mapping_configurations'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    oracle_connection_id = db.Column(db.Integer, db.ForeignKey('oracle_connections.id'), nullable=False)
    elasticsearch_connection_id = db.Column(db.Integer, db.ForeignKey('elasticsearch_connections.id'), nullable=False)
    oracle_query = db.Column(db.Text, nullable=False)
    elasticsearch_index = db.Column(db.String(255), nullable=False)
    field_mappings = db.Column(db.Text, nullable=False)  # JSON string
    transformation_rules = db.Column(db.Text)  # JSON string
    mapping_metadata = db.Column(db.Text)  # JSON field for advanced mapping metadata
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    # Relationships
    oracle_connection = db.relationship('OracleConnection', backref='mappings')
    elasticsearch_connection = db.relationship('ElasticsearchConnection', backref='mappings')
    
    def get_field_mappings(self):
        try:
            return json.loads(self.field_mappings) if self.field_mappings else []
        except:
            return []
    
    def set_field_mappings(self, mappings):
        self.field_mappings = json.dumps(mappings)
    
    def get_transformation_rules(self):
        try:
            return json.loads(self.transformation_rules) if self.transformation_rules else []
        except:
            return []
    
    def set_transformation_rules(self, rules):
        self.transformation_rules = json.dumps(rules)
    
    def get_mapping_metadata(self):
        try:
            return json.loads(self.mapping_metadata) if self.mapping_metadata else {}
        except:
            return {}
    
    def set_mapping_metadata(self, metadata):
        self.mapping_metadata = json.dumps(metadata)

class MigrationJob(db.Model):
    __tablename__ = 'migration_jobs'
    
    id = db.Column(db.Integer, primary_key=True)
    mapping_configuration_id = db.Column(db.Integer, db.ForeignKey('mapping_configurations.id'), nullable=False)
    status = db.Column(db.String(50), nullable=False, default='pending')  # pending, running, completed, failed
    total_records = db.Column(db.Integer, default=0)
    processed_records = db.Column(db.Integer, default=0)
    failed_records = db.Column(db.Integer, default=0)
    start_time = db.Column(db.DateTime)
    end_time = db.Column(db.DateTime)
    error_message = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationship
    mapping_configuration = db.relationship('MappingConfiguration', backref='migration_jobs')
    
    @property
    def progress_percentage(self):
        if self.total_records > 0:
            return (self.processed_records / self.total_records) * 100
        return 0
