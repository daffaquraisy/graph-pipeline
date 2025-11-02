"""
ETL Script: PostgreSQL to Neo4j - Database-Driven Configuration
================================================================
Features:
- Single combined Cypher output file
- No hardcoded mappings (reads from control_db tables)
- Windows compatible (no emoji characters)
- Fully commented code
- Requires .env file for credentials
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from neo4j import GraphDatabase
from datetime import datetime
import os
from typing import List, Dict, Any, Optional, Set
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging with UTF-8 encoding for Windows compatibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_process.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ETLConfig:
    """Configuration for ETL control database connection"""
    
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
    
    @classmethod
    def from_env(cls):
        """
        Create config from environment variables.
        Raises ValueError if required variables are missing.
        """
        # Get required environment variables
        host = os.getenv("ETL_DB_HOST")
        port = os.getenv("ETL_DB_PORT")
        dbname = os.getenv("ETL_DB_NAME")
        user = os.getenv("ETL_DB_USER")
        password = os.getenv("ETL_DB_PASSWORD")
        
        # Validate that all required variables are present
        missing = []
        if not host:
            missing.append("ETL_DB_HOST")
        if not port:
            missing.append("ETL_DB_PORT")
        if not dbname:
            missing.append("ETL_DB_NAME")
        if not user:
            missing.append("ETL_DB_USER")
        if not password:
            missing.append("ETL_DB_PASSWORD")
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        return cls(
            host=host,
            port=int(port),
            dbname=dbname,
            user=user,
            password=password
        )

class PostgresToNeo4jETL:
    """Main ETL class that orchestrates data extraction and Cypher generation"""
    
    def __init__(self, control_db_config: ETLConfig, output_file: str = "./complete_graph.cypher"):
        self.control_db_config = control_db_config
        self.output_file = output_file
        self.control_conn = None
        self.current_log_id = None
        self.cypher_output = []  # Stores all Cypher statements
        
        # Mappings loaded from database
        self.label_mappings = {}  # table_name -> node_label
        self.relationship_mappings = {}  # source_db -> list of relationships
    
    def load_label_mappings(self):
        """Load node label mappings from control database"""
        try:
            with self.control_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT table_name, node_label 
                    FROM node_label_mappings 
                    WHERE is_active = true
                """)
                mappings = cur.fetchall()
                
                # Build mapping dictionary
                for mapping in mappings:
                    self.label_mappings[mapping['table_name']] = mapping['node_label']
                
                logger.info(f"[OK] Loaded {len(self.label_mappings)} node label mappings")
        except Exception as e:
            logger.error(f"[ERROR] Failed to load label mappings: {e}")
            raise
    
    def load_relationship_mappings(self):
        """Load relationship definitions from control database"""
        try:
            with self.control_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT source_db, relationship_type, from_label, to_label, 
                           join_condition, junction_table, junction_label, 
                           description, execution_order
                    FROM relationship_mappings 
                    WHERE is_active = true
                    ORDER BY execution_order
                """)
                mappings = cur.fetchall()
                
                # Organize by source database
                for mapping in mappings:
                    source_db = mapping['source_db']
                    if source_db not in self.relationship_mappings:
                        self.relationship_mappings[source_db] = []
                    self.relationship_mappings[source_db].append(dict(mapping))
                
                total_relationships = sum(len(rels) for rels in self.relationship_mappings.values())
                logger.info(f"[OK] Loaded {total_relationships} relationship definitions")
        except Exception as e:
            logger.error(f"[ERROR] Failed to load relationship mappings: {e}")
            raise
    
    def get_node_label(self, table_name: str) -> str:
        """
        Get Neo4j node label for a table.
        Falls back to capitalized table name if not in mappings.
        """
        return self.label_mappings.get(table_name, table_name.capitalize())
    
    def connect_to_control_db(self):
        """Establish connection to the ETL control database"""
        try:
            self.control_conn = psycopg2.connect(
                host=self.control_db_config.host,
                port=self.control_db_config.port,
                dbname=self.control_db_config.dbname,
                user=self.control_db_config.user,
                password=self.control_db_config.password
            )
            logger.info(f"[OK] Connected to control database: {self.control_db_config.dbname}")
            
            # Load mappings from database
            self.load_label_mappings()
            self.load_relationship_mappings()
            
            return True
        except Exception as e:
            logger.error(f"[ERROR] Failed to connect to control database: {e}")
            return False
    
    def start_etl_run(self) -> Optional[int]:
        """Create a new ETL run log entry"""
        try:
            with self.control_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO etl_run_logs (run_start_time, status)
                    VALUES (NOW(), 'running')
                    RETURNING log_id
                """)
                log_id = cur.fetchone()[0]
                self.control_conn.commit()
                self.current_log_id = log_id
                logger.info(f"[OK] Started ETL run with log_id: {log_id}")
                return log_id
        except Exception as e:
            logger.error(f"[ERROR] Failed to start ETL run: {e}")
            self.control_conn.rollback()
            return None
    
    def complete_etl_run(self, status: str = 'completed'):
        """Mark the ETL run as completed or failed"""
        if not self.current_log_id:
            return
        
        try:
            with self.control_conn.cursor() as cur:
                cur.execute("""
                    UPDATE etl_run_logs
                    SET run_end_time = NOW(), status = %s
                    WHERE log_id = %s
                """, (status, self.current_log_id))
                self.control_conn.commit()
                logger.info(f"[OK] Completed ETL run with status: {status}")
        except Exception as e:
            logger.error(f"[ERROR] Failed to complete ETL run: {e}")
            self.control_conn.rollback()
    
    def get_active_sources(self) -> List[Dict[str, Any]]:
        """Retrieve all active source databases from configuration"""
        try:
            with self.control_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT source_id, source_name, db_host, db_port, db_name, 
                           db_user, db_password
                    FROM source_databases
                    WHERE is_active = true
                    ORDER BY source_id
                """)
                sources = cur.fetchall()
                logger.info(f"[OK] Found {len(sources)} active source database(s)")
                return [dict(row) for row in sources]
        except Exception as e:
            logger.error(f"[ERROR] Failed to retrieve source databases: {e}")
            return []
    
    def get_exclusion_rules(self, source_id: int) -> Dict[str, Set[str]]:
        """
        Get field exclusion rules for a specific source database.
        Returns a dictionary mapping table names to sets of excluded column names.
        """
        try:
            with self.control_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT table_name, column_name
                    FROM field_exclusion_rules
                    WHERE source_id = %s AND is_excluded = true
                """, (source_id,))
                rules = cur.fetchall()
                
                # Organize exclusions by table
                exclusions = {}
                for rule in rules:
                    table = rule['table_name']
                    column = rule['column_name']
                    if table not in exclusions:
                        exclusions[table] = set()
                    exclusions[table].add(column)
                
                if exclusions:
                    total_excluded = sum(len(cols) for cols in exclusions.values())
                    logger.info(f"[OK] Loaded exclusion rules for source_id {source_id}: {total_excluded} columns excluded")
                return exclusions
        except Exception as e:
            logger.error(f"[ERROR] Failed to get exclusion rules: {e}")
            return {}
    
    def create_table_log(self, source_id: int, table_name: str) -> Optional[int]:
        """Create a log entry for processing a specific table"""
        try:
            with self.control_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO etl_table_logs 
                    (log_id, source_id, table_name, status, is_progressing, start_time)
                    VALUES (%s, %s, %s, 'running', true, NOW())
                    RETURNING table_log_id
                """, (self.current_log_id, source_id, table_name))
                table_log_id = cur.fetchone()[0]
                self.control_conn.commit()
                return table_log_id
        except Exception as e:
            logger.error(f"[ERROR] Failed to create table log: {e}")
            self.control_conn.rollback()
            return None
    
    def update_table_log(self, table_log_id: int, status: str, 
                        rows_processed: int = 0, error_message: str = None):
        """Update the table log with completion status"""
        try:
            with self.control_conn.cursor() as cur:
                cur.execute("""
                    UPDATE etl_table_logs
                    SET status = %s, is_progressing = false, is_done = true,
                        end_time = NOW(), rows_processed = %s, error_message = %s
                    WHERE table_log_id = %s
                """, (status, rows_processed, error_message, table_log_id))
                self.control_conn.commit()
        except Exception as e:
            logger.error(f"[ERROR] Failed to update table log: {e}")
            self.control_conn.rollback()
    
    def connect_to_source(self, source_config: Dict[str, Any]):
        """Connect to a source database"""
        try:
            conn = psycopg2.connect(
                host=source_config['db_host'],
                port=source_config['db_port'],
                dbname=source_config['db_name'],
                user=source_config['db_user'],
                password=source_config['db_password']
            )
            logger.info(f"[OK] Connected to source: {source_config['source_name']}")
            
            # Update last_accessed timestamp in control database
            with self.control_conn.cursor() as cur:
                cur.execute("""
                    UPDATE source_databases
                    SET last_accessed = NOW()
                    WHERE source_id = %s
                """, (source_config['source_id'],))
                self.control_conn.commit()
            
            return conn
        except Exception as e:
            logger.error(f"[ERROR] Failed to connect to source {source_config['source_name']}: {e}")
            return None
    
    def get_table_names(self, source_conn) -> List[str]:
        """Get all table names from a source database (excluding system tables)"""
        try:
            with source_conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cur.fetchall()]
                logger.info(f"[OK] Found {len(tables)} tables in source database")
                return tables
        except Exception as e:
            logger.error(f"[ERROR] Failed to get table names: {e}")
            return []
    
    def get_table_columns(self, source_conn, table_name: str, 
                         excluded_columns: Set[str]) -> List[str]:
        """
        Get column names for a table, excluding specified columns.
        Returns list of column names in ordinal position order.
        """
        try:
            with source_conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                all_columns = [row[0] for row in cur.fetchall()]
                
                # Filter out excluded columns
                included_columns = [col for col in all_columns if col not in excluded_columns]
                
                if excluded_columns:
                    excluded_list = [col for col in all_columns if col in excluded_columns]
                    logger.info(f"  Excluded columns: {excluded_list}")
                
                return included_columns
        except Exception as e:
            logger.error(f"[ERROR] Failed to get columns for table {table_name}: {e}")
            return []
    
    def get_primary_key(self, source_conn, table_name: str) -> Optional[str]:
        """Get the primary key column name for a table"""
        try:
            with source_conn.cursor() as cur:
                cur.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary
                """, (table_name,))
                result = cur.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"[ERROR] Failed to get primary key for {table_name}: {e}")
            return None
    
    def extract_table_data(self, source_conn, table_name: str, columns: List[str]) -> List[Dict]:
        """Extract all rows from a table with specified columns"""
        try:
            column_list = ", ".join(columns)
            query = f"SELECT {column_list} FROM {table_name}"
            
            with source_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                rows = cur.fetchall()
                logger.info(f"  Extracted {len(rows)} rows from {table_name}")
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"[ERROR] Failed to extract data from {table_name}: {e}")
            return []
    
    def sanitize_value_for_cypher(self, value: Any) -> str:
        """
        Convert Python values to Cypher-compatible string representation.
        Handles NULL, boolean, numeric, string, and datetime types.
        """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Escape backslashes and single quotes for Cypher
            escaped = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        elif isinstance(value, datetime):
            return f"datetime('{value.isoformat()}')"
        else:
            # Convert other types to string and escape
            escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
    
    def generate_cypher_for_table(self, source_name: str, table_name: str, 
                                  rows: List[Dict], columns: List[str],
                                  primary_key: Optional[str]):
        """
        Generate Cypher CREATE statements for nodes.
        Adds constraint on primary key if available.
        """
        node_label = self.get_node_label(table_name)
        
        # Add header comment
        self.cypher_output.append(f"// ============================================")
        self.cypher_output.append(f"// Source: {source_name}")
        self.cypher_output.append(f"// Table: {table_name}")
        self.cypher_output.append(f"// Node Label: {node_label}")
        self.cypher_output.append(f"// Rows: {len(rows)}")
        self.cypher_output.append(f"// Generated: {datetime.now().isoformat()}")
        self.cypher_output.append(f"// ============================================\n")
        
        # Create constraint on primary key
        if primary_key and primary_key in columns:
            constraint_name = f"constraint_{table_name}_{primary_key}"
            self.cypher_output.append(
                f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
                f"FOR (n:{node_label}) "
                f"REQUIRE n.{primary_key} IS UNIQUE;\n"
            )
        
        # Generate CREATE statement for each row
        for row in rows:
            properties = []
            for col in columns:
                if col in row and row[col] is not None:
                    value = self.sanitize_value_for_cypher(row[col])
                    properties.append(f"{col}: {value}")
            
            if properties:
                properties_str = ", ".join(properties)
                self.cypher_output.append(
                    f"CREATE (:{node_label} {{{properties_str}}});"
                )
        
        self.cypher_output.append("")  # Empty line after table
    
    def generate_relationships(self, source_name: str):
        """
        Generate Cypher MATCH/CREATE statements for relationships.
        Uses relationships defined in relationship_mappings table.
        """
        # Check if there are any relationships for this source
        if source_name not in self.relationship_mappings:
            logger.info(f"  No relationships defined for {source_name}")
            return
        
        self.cypher_output.append("// ============================================")
        self.cypher_output.append(f"// Relationships for: {source_name}")
        self.cypher_output.append(f"// Generated: {datetime.now().isoformat()}")
        self.cypher_output.append("// ============================================\n")
        
        # Generate Cypher for each relationship
        for rel in self.relationship_mappings[source_name]:
            # Add description comment
            if rel['description']:
                self.cypher_output.append(f"// {rel['description']}")
            
            # Debug: log the relationship details
            logger.info(f"  Relationship: {rel['relationship_type']} | Junction: {rel.get('junction_table')}")
            
            from_var = rel['from_label'].lower()[0]
            to_var = rel['to_label'].lower()[0]
            
            # Handle special variable naming
            if rel['to_label'] == 'PaymentMethod':
                to_var = 'pm'
            elif rel['to_label'] == 'PlayEvent':
                to_var = 'pe'
            elif rel['to_label'] == 'Tag':
                to_var = 'tag'
            
            # Check if this uses a junction table (many-to-many relationship)
            if rel.get('junction_table') and rel.get('junction_label'):
                # Junction table relationship
                self.cypher_output.append(
                    f"MATCH ({from_var}:{rel['from_label']}), "
                    f"(link:{rel['junction_label']}), "
                    f"({to_var}:{rel['to_label']})"
                )
            else:
                # Direct relationship
                self.cypher_output.append(
                    f"MATCH ({from_var}:{rel['from_label']}), ({to_var}:{rel['to_label']})"
                )
            
            self.cypher_output.append(f"WHERE {rel['join_condition']}")
            self.cypher_output.append(f"CREATE ({from_var})-[:{rel['relationship_type']}]->({to_var});\n")
    
    def process_source_database(self, source_config: Dict[str, Any]):
        """
        Process a single source database:
        1. Connect to source
        2. Extract data from all tables
        3. Generate Cypher for nodes
        4. Generate Cypher for relationships
        """
        source_id = source_config['source_id']
        source_name = source_config['source_name']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing source: {source_name} (ID: {source_id})")
        logger.info(f"{'='*60}")
        
        # Connect to source database
        source_conn = self.connect_to_source(source_config)
        if not source_conn:
            return
        
        try:
            # Load configuration for this source
            exclusion_rules = self.get_exclusion_rules(source_id)
            tables = self.get_table_names(source_conn)
            
            # Add database header to output
            self.cypher_output.append(f"\n// ========================================")
            self.cypher_output.append(f"// DATABASE: {source_name}")
            self.cypher_output.append(f"// Total tables: {len(tables)}")
            self.cypher_output.append(f"// ========================================\n")
            
            # Process each table
            for table_name in tables:
                logger.info(f"\n-> Processing table: {table_name}")
                table_log_id = self.create_table_log(source_id, table_name)
                
                try:
                    # Get columns (respecting exclusions)
                    excluded_cols = exclusion_rules.get(table_name, set())
                    columns = self.get_table_columns(source_conn, table_name, excluded_cols)
                    
                    if not columns:
                        logger.warning(f"  [WARN] No columns available for {table_name}, skipping")
                        self.update_table_log(table_log_id, 'failed', 0, 
                                            'No columns available after exclusions')
                        continue
                    
                    # Get primary key for constraint creation
                    primary_key = self.get_primary_key(source_conn, table_name)
                    if primary_key:
                        logger.info(f"  Primary key: {primary_key}")
                    
                    # Extract data
                    rows = self.extract_table_data(source_conn, table_name, columns)
                    
                    # Generate Cypher
                    self.generate_cypher_for_table(source_name, table_name, rows, columns, primary_key)
                    
                    # Update log as completed
                    self.update_table_log(table_log_id, 'completed', len(rows))
                    logger.info(f"  [OK] Completed: {table_name} ({len(rows)} rows)")
                    
                except Exception as e:
                    error_msg = f"Error processing table {table_name}: {str(e)}"
                    logger.error(f"  [ERROR] {error_msg}")
                    self.update_table_log(table_log_id, 'failed', 0, error_msg)
            
            # Generate relationships for this source
            logger.info(f"\n-> Generating relationships for {source_name}")
            self.generate_relationships(source_name)
            
        finally:
            source_conn.close()
            logger.info(f"[OK] Disconnected from source: {source_name}")
    
    def write_output_file(self):
        """Write all accumulated Cypher statements to a single file"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                # Write file header
                f.write("// ========================================\n")
                f.write("// COMPLETE GRAPH DATABASE IMPORT\n")
                f.write(f"// Generated: {datetime.now().isoformat()}\n")
                f.write("// ========================================\n\n")
                
                # Write all Cypher statements
                f.write("\n".join(self.cypher_output))
            
            # Get file size
            file_size_kb = os.path.getsize(self.output_file) // 1024
            
            # Register the Cypher script in database
            self.register_cypher_script(self.output_file, file_size_kb)
            
            logger.info(f"\n[OK] Complete Cypher file generated: {self.output_file}")
            logger.info(f"[OK] File size: {file_size_kb} KB")
            logger.info(f"[OK] Total lines: {len(self.cypher_output)}")
        except Exception as e:
            logger.error(f"[ERROR] Failed to write output file: {e}")
    
    def register_cypher_script(self, file_path: str, file_size_kb: int):
        """Register the generated Cypher script in cypher_scripts table"""
        try:
            script_name = os.path.basename(file_path)
            abs_path = os.path.abspath(file_path)
            
            with self.control_conn.cursor() as cur:
                # Insert or update the script record
                cur.execute("""
                    INSERT INTO cypher_scripts 
                    (script_name, file_path, file_size_kb, last_updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (script_name) 
                    DO UPDATE SET 
                        file_path = EXCLUDED.file_path,
                        file_size_kb = EXCLUDED.file_size_kb,
                        last_updated_at = NOW()
                    RETURNING script_id
                """, (script_name, abs_path, file_size_kb))
                script_id = cur.fetchone()[0]
                self.control_conn.commit()
                logger.info(f"[OK] Registered Cypher script in database (ID: {script_id})")
        except Exception as e:
            logger.error(f"[ERROR] Failed to register Cypher script: {e}")
            self.control_conn.rollback()
    
    def run(self):
        """Main ETL execution method"""
        logger.info("\n" + "="*60)
        logger.info("Starting PostgreSQL to Neo4j ETL Process")
        logger.info("="*60 + "\n")
        
        # Connect to control database and load mappings
        if not self.connect_to_control_db():
            logger.error("[ERROR] Cannot proceed without control database connection")
            return
        
        try:
            # Start ETL run logging
            if not self.start_etl_run():
                logger.error("[ERROR] Failed to start ETL run")
                return
            
            # Get active source databases
            sources = self.get_active_sources()
            
            if not sources:
                logger.warning("[WARN] No active source databases found")
                self.complete_etl_run('completed')
                return
            
            # Process each source database
            for source in sources:
                try:
                    self.process_source_database(source)
                except Exception as e:
                    logger.error(f"[ERROR] Error processing source {source['source_name']}: {e}")
            
            # Write final combined output file
            self.write_output_file()
            
            # Mark ETL as completed
            self.complete_etl_run('completed')
            
            logger.info("\n" + "="*60)
            logger.info("ETL Process Completed Successfully")
            logger.info("="*60)
            logger.info(f"\nLoad this file into Neo4j: {self.output_file}")
            
        except Exception as e:
            logger.error(f"\n[ERROR] ETL process failed: {e}")
            self.complete_etl_run('failed')
        
        finally:
            if self.control_conn:
                self.control_conn.close()
                logger.info("\n[OK] Disconnected from control database")

class Neo4jLoader:
    """Loads Cypher scripts into Neo4j Aura"""
    
    def __init__(self, control_db_config, neo4j_uri, neo4j_user, neo4j_password):
        self.control_db_config = control_db_config
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.control_conn = None
        self.neo4j_driver = None
    
    def connect_to_control_db(self):
        """Connect to ETL control database"""
        try:
            self.control_conn = psycopg2.connect(
                host=self.control_db_config.host,
                port=self.control_db_config.port,
                dbname=self.control_db_config.dbname,
                user=self.control_db_config.user,
                password=self.control_db_config.password
            )
            logger.info(f"[OK] Connected to control database")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Failed to connect to control database: {e}")
            return False
    
    def connect_to_neo4j(self):
        """Connect to Neo4j Aura"""
        try:
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_uri,
                auth=(self.neo4j_user, self.neo4j_password)
            )
            # Test connection
            self.neo4j_driver.verify_connectivity()
            logger.info(f"[OK] Connected to Neo4j Aura: {self.neo4j_uri}")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Failed to connect to Neo4j: {e}")
            return False
    
    def get_latest_cypher_script(self):
        """Get the latest Cypher script from cypher_scripts table"""
        try:
            with self.control_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT script_id, script_name, file_path, file_size_kb, last_updated_at
                    FROM cypher_scripts
                    ORDER BY last_updated_at DESC
                    LIMIT 1
                """)
                script = cur.fetchone()
                
                if script:
                    logger.info(f"[OK] Found latest script: {script['script_name']}")
                    logger.info(f"     Path: {script['file_path']}")
                    logger.info(f"     Size: {script['file_size_kb']} KB")
                    logger.info(f"     Updated: {script['last_updated_at']}")
                    return dict(script)
                else:
                    logger.warning("[WARN] No Cypher scripts found in database")
                    return None
        except Exception as e:
            logger.error(f"[ERROR] Failed to get latest script: {e}")
            return None
    
    def read_cypher_file(self, file_path):
        """Read Cypher file and split into individual statements"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Split by semicolons (end of statements)
            statements = []
            for line in content.split('\n'):
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('//'):
                    statements.append(line)
            
            # Join lines that are part of the same statement
            combined_statements = []
            current_statement = []
            
            for line in statements:
                current_statement.append(line)
                if line.endswith(';'):
                    combined_statements.append(' '.join(current_statement))
                    current_statement = []
            
            logger.info(f"[OK] Read {len(combined_statements)} Cypher statements from file")
            return combined_statements
        except Exception as e:
            logger.error(f"[ERROR] Failed to read Cypher file: {e}")
            return []
    
    def clear_neo4j_database(self):
        """Clear all nodes and relationships in Neo4j (optional)"""
        try:
            with self.neo4j_driver.session() as session:
                # Delete all relationships first
                session.run("MATCH ()-[r]->() DELETE r")
                # Delete all nodes
                session.run("MATCH (n) DELETE n")
                logger.info("[OK] Cleared Neo4j database")
        except Exception as e:
            logger.error(f"[ERROR] Failed to clear database: {e}")
    
    def execute_cypher_statements(self, statements):
        """Execute Cypher statements in Neo4j"""
        success_count = 0
        error_count = 0
        
        with self.neo4j_driver.session() as session:
            for i, statement in enumerate(statements, 1):
                try:
                    session.run(statement)
                    success_count += 1
                    
                    # Log progress every 100 statements
                    if i % 100 == 0:
                        logger.info(f"[PROGRESS] Executed {i}/{len(statements)} statements")
                
                except Exception as e:
                    error_count += 1
                    logger.error(f"[ERROR] Statement {i} failed: {str(e)[:100]}")
                    logger.error(f"         Statement: {statement[:100]}...")
        
        return success_count, error_count
    
    def update_script_run_time(self, script_id):
        """Update last_run_time in cypher_scripts table"""
        try:
            with self.control_conn.cursor() as cur:
                cur.execute("""
                    UPDATE cypher_scripts
                    SET last_run_time = NOW()
                    WHERE script_id = %s
                """, (script_id,))
                self.control_conn.commit()
                logger.info(f"[OK] Updated script run time")
        except Exception as e:
            logger.error(f"[ERROR] Failed to update run time: {e}")
            self.control_conn.rollback()
    
    def load(self, clear_database=False):
        """Main load process"""
        logger.info("\n" + "="*60)
        logger.info("Starting Neo4j Loader")
        logger.info("="*60 + "\n")
        
        # Connect to databases
        if not self.connect_to_control_db():
            return False
        
        if not self.connect_to_neo4j():
            return False
        
        try:
            # Get latest Cypher script
            script = self.get_latest_cypher_script()
            if not script:
                logger.error("[ERROR] No Cypher script to load")
                return False
            
            # Check if file exists
            if not os.path.exists(script['file_path']):
                logger.error(f"[ERROR] File not found: {script['file_path']}")
                return False
            
            # Optionally clear database
            if clear_database:
                logger.info("\n[WARN] Clearing Neo4j database...")
                self.clear_neo4j_database()
            
            # Read Cypher file
            logger.info(f"\n[LOADING] Reading Cypher file...")
            statements = self.read_cypher_file(script['file_path'])
            
            if not statements:
                logger.error("[ERROR] No statements to execute")
                return False
            
            # Execute statements
            logger.info(f"\n[LOADING] Executing {len(statements)} statements in Neo4j...")
            success_count, error_count = self.execute_cypher_statements(statements)
            
            # Update run time
            self.update_script_run_time(script['script_id'])
            
            # Summary
            logger.info("\n" + "="*60)
            logger.info("Load Complete")
            logger.info("="*60)
            logger.info(f"Successful: {success_count}")
            logger.info(f"Failed: {error_count}")
            logger.info(f"Total: {len(statements)}")
            
            if error_count > 0:
                logger.warning(f"\n[WARN] {error_count} statements failed. Check neo4j_loader.log for details.")
            
            return error_count == 0
            
        except Exception as e:
            logger.error(f"\n[ERROR] Load failed: {e}")
            return False
        
        finally:
            if self.control_conn:
                self.control_conn.close()
            if self.neo4j_driver:
                self.neo4j_driver.close()

def main():
    """Entry point for the ETL script"""
    
    try:
        # Load configuration from environment variables
        # This will raise ValueError if any required variable is missing
        control_db_config = ETLConfig.from_env()
        
        # Initialize and run ETL
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        etl = PostgresToNeo4jETL(
            control_db_config=control_db_config,
            output_file=f"./graph_output_{timestamp}.cypher" 
        )
        
        etl.run()

        neo4j_uri = os.getenv('NEO4J_URI')
        neo4j_user = os.getenv('NEO4J_USERNAME', 'neo4j')
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        
        # Initialize loader
        loader = Neo4jLoader(
            control_db_config=control_db_config,
            neo4j_uri=neo4j_uri,
            neo4j_user=neo4j_user,
            neo4j_password=neo4j_password
        )
        
        # Load with option to clear database first
        # Set to True if you want to start fresh, False to append
        clear_database = True  # Change this as needed
        
        loader.load(clear_database=clear_database)

        
    except ValueError as e:
        logger.error(f"[ERROR] Configuration error: {e}")
        logger.error("[ERROR] Please create a .env file with all required variables")
        logger.error("[ERROR] Required: ETL_DB_HOST, ETL_DB_PORT, ETL_DB_NAME, ETL_DB_USER, ETL_DB_PASSWORD")
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")


if __name__ == "__main__":
    main()