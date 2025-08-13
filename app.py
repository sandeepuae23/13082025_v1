import os
import logging
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix

# Configure logging
logging.basicConfig(level=logging.DEBUG)

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)

# Create the app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Configure the database
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL", "sqlite:///mapping_config.db")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}

# Initialize the app with the extension
db.init_app(app)

# Register blueprints
from routes.main import main_bp
from routes.oracle import oracle_bp
from routes.elasticsearch import elasticsearch_bp
from routes.mapping import mapping_bp
from routes.migration import migration_bp
from routes.advanced_migration import advanced_migration_bp

app.register_blueprint(main_bp)
app.register_blueprint(oracle_bp, url_prefix='/api/oracle')
app.register_blueprint(elasticsearch_bp, url_prefix='/api/elasticsearch')
app.register_blueprint(mapping_bp, url_prefix='/api/mapping')
app.register_blueprint(migration_bp, url_prefix='/api/migration')
app.register_blueprint(advanced_migration_bp, url_prefix='/api/migration')

with app.app_context():
    # Import models to ensure tables are created
    import models
    db.create_all()
