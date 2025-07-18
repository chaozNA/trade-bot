import sqlite3
import logging
import os
from typing import List, Dict, Optional, Tuple
from contextlib import contextmanager

class DBClient:
    def __init__(self, db_path='data/bot.db'):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.init_db()

    def init_db(self):
        """
        Initializes the database. For this prototype, it drops and recreates 
        trade-related tables on every startup to ensure a clean state, but 
        preserves the 'messages' table.
        """
        self.logger.info("Initializing database... Ensuring tables exist.")
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()

                # --- Recreate all tables

                # Create message_analyses table with new schema
                self.logger.debug("Creating 'message_analyses' table with new schema...")
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS message_analyses (
                        analysis_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                        message_id          INTEGER NOT NULL UNIQUE,
                        classification      TEXT NOT NULL,
                        related_trade_id    INTEGER,
                        reason              TEXT,
                        confidence_score    INTEGER,
                        analysis_payload    TEXT, -- Full JSON payload for audit
                        created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (message_id) REFERENCES messages(message_id),
                        FOREIGN KEY (related_trade_id) REFERENCES trades(trade_id)
                    )
                ''')

                # Create trades table
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        trade_id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        opening_analysis_id INTEGER NOT NULL,
                        client_order_id     TEXT NOT NULL UNIQUE,
                        broker_id           TEXT,
                        symbol              TEXT NOT NULL,
                        option_type         TEXT NOT NULL CHECK(option_type IN ('CALL', 'PUT')),
                        strike              REAL NOT NULL,
                        expiration          DATE NOT NULL,
                        status              TEXT NOT NULL,
                        quantity            REAL NOT NULL,
                        target_entry_price  REAL,
                        filled_entry_price  REAL,
                        filled_exit_price   REAL,
                        stop_loss           REAL,
                        take_profit         REAL,
                        created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        opened_at           TIMESTAMP,
                        closed_at           TIMESTAMP,
                        FOREIGN KEY (opening_analysis_id) REFERENCES message_analyses(analysis_id)
                    )
                ''')

                # Create trade_history table
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS trade_history (
                        history_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id            INTEGER NOT NULL,
                        triggering_analysis_id INTEGER, 
                        timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        event_type          TEXT NOT NULL,
                        details             TEXT, -- JSON
                        FOREIGN KEY (trade_id) REFERENCES trades(trade_id),
                        FOREIGN KEY (triggering_analysis_id) REFERENCES message_analyses(analysis_id)
                    )
                ''')

                self.logger.info(f"Database tables re-initialized successfully at {self.db_path}")

        except sqlite3.Error as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """Create and return a new database connection with proper settings."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('PRAGMA busy_timeout=5000')  # 5 second timeout
            yield conn
        except sqlite3.Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    self.logger.error(f"Error closing database connection: {e}")

    def _execute_in_transaction(self, query: str, params: Tuple = ()):
        """Execute a query in a transaction with proper error handling."""
        with self.get_connection() as conn:
            try:
                with conn:  # This creates a transaction
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    return cursor
            except sqlite3.Error as e:
                self.logger.error(f"DB execute error: {e}")
                raise

    def execute(self, query: str, params: Tuple = ()) -> None:
        """Execute a query that doesn't return results."""
        self._execute_in_transaction(query, params)

    def insert_and_get_id(self, query: str, params: Tuple = ()) -> Optional[int]:
        """Execute an insert query and return the last inserted row id."""
        try:
            with self.get_connection() as conn:
                with conn:  # Transaction starts
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    return cursor.lastrowid
        except sqlite3.Error as e:
            self.logger.error(f"DB insert error: {e}")
            return None

    def fetchone(self, query: str, params: Tuple = ()) -> Optional[Dict]:
        """Fetch a single row from the database."""
        try:
            with self.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                with conn:  # Transaction starts
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    result = cursor.fetchone()
            return dict(result) if result else None
        except sqlite3.Error as e:
            self.logger.error(f"DB fetchone error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in fetchone: {e}")
            return None

    def fetchall(self, query: str, params: Tuple = ()) -> List[Dict]:
        """Fetch all rows from the database."""
        try:
            with self.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                with conn:  # Transaction starts
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            self.logger.error(f"DB fetchall error: {e}")
            return []

    def close(self):
        """Close any open database connections."""
        # SQLite connections are closed automatically when they go out of scope,
        # but we'll keep this method for API compatibility
        pass

db_client = DBClient()