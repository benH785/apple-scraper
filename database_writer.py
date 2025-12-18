#!/usr/bin/env python3
"""
Database writer module for Apple Refurb scraper
Provides dual-write capability to PostgreSQL (Supabase)
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseWriter:
    """Handles writing Apple refurb data to PostgreSQL database."""
    
    def __init__(self):
        """Initialize database connection."""
        self.connection = None
        self.enabled = False
        self.connect()
    
    def connect(self):
        """Connect to the PostgreSQL database."""
        try:
            database_url = os.getenv('DATABASE_URL')
            if not database_url:
                print("⚠️ DATABASE_URL not set - database writes disabled")
                return
            
            self.connection = psycopg2.connect(database_url)
            self.enabled = True
            print("✅ Database connection established")
        except Exception as e:
            print(f"⚠️ Database connection failed - continuing with Sheets only: {e}")
            self.enabled = False
    
    def write_to_apple_history(self, products: List[Dict]) -> int:
        """
        Write standardized products to apple_history table.
        
        Args:
            products: List of standardized product dictionaries
            
        Returns:
            Number of records inserted
        """
        if not self.enabled or not self.connection:
            return 0
        
        inserted = 0
        cursor = None
        
        try:
            cursor = self.connection.cursor()
            
            insert_query = """
                INSERT INTO apple_history (
                    title, url, condition, grade, price, price_change, seller,
                    machine, model, year, cpu, cpu_cores, storage, ram, gpu, colour,
                    variant_id, warning, scraped_at, screen
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            for product in products:
                try:
                    # Parse price (remove currency symbols)
                    price = None
                    if product.get('Price'):
                        price_str = str(product['Price']).replace('£', '').replace(',', '')
                        try:
                            price = float(price_str)
                        except:
                            pass
                    
                    # Parse year
                    year = None
                    if product.get('Year'):
                        try:
                            year = int(product['Year'])
                        except:
                            pass
                    
                    # Parse timestamp
                    scraped_at = datetime.now()
                    if product.get('Timestamp'):
                        try:
                            # Try to parse various timestamp formats
                            timestamp_str = product['Timestamp']
                            # Format: "2025-08-05 15:42:00"
                            scraped_at = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                        except:
                            # Format: "2025-08-05"
                            try:
                                scraped_at = datetime.strptime(timestamp_str, "%Y-%m-%d")
                            except:
                                pass
                    
                    values = (
                        product.get('Title', ''),
                        product.get('URL'),
                        product.get('Condition', 'Refurbished'),
                        product.get('Grade', 'Excellent'),
                        price,
                        product.get('Change'),
                        product.get('Seller', 'Apple'),
                        product.get('Machine'),
                        product.get('Model'),
                        year,
                        product.get('CPU'),
                        product.get('CPU Cores'),
                        product.get('HD (GB)'),  # storage
                        product.get('RAM (GB)'),  # ram
                        product.get('GPU'),
                        product.get('Colour'),
                        product.get('Variant ID'),
                        product.get('Warning'),
                        scraped_at,
                        product.get('Screen')  # For Studio Display glass type
                    )
                    
                    cursor.execute(insert_query, values)
                    inserted += 1
                    
                except Exception as e:
                    print(f"⚠️ Failed to insert product '{product.get('Title', 'Unknown')}': {e}")
                    continue
            
            # Commit all inserts
            self.connection.commit()
            print(f"💾 Inserted {inserted} records to database")
            
        except Exception as e:
            print(f"❌ Database write failed: {e}")
            if self.connection:
                self.connection.rollback()
        finally:
            if cursor:
                cursor.close()
        
        return inserted
    
    def close(self):
        """Close database connection."""
        if self.connection:
            try:
                self.connection.close()
                print("🔌 Database connection closed")
            except:
                pass
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close()


# Test function
if __name__ == "__main__":
    # Test with sample data
    db = DatabaseWriter()
    
    if db.enabled:
        test_product = [{
            'Title': 'Test MacBook Pro',
            'URL': 'https://apple.com/test',
            'Condition': 'Refurbished',
            'Grade': 'Excellent',
            'Price': '1299',
            'Machine': 'MacBook Pro',
            'Model': 'MacBook Pro 14-inch',
            'Year': '2024',
            'CPU': 'M3',
            'CPU Cores': '8',
            'HD (GB)': '512',
            'RAM (GB)': '16',
            'GPU': '10-Core',
            'Colour': 'Space Gray',
            'Variant ID': 'TEST123',
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }]
        
        count = db.write_to_apple_history(test_product)
        print(f"Test complete: {count} records inserted")
        
        db.close()
    else:
        print("Database not enabled - check DATABASE_URL environment variable")
