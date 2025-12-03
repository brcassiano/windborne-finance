from flask import Flask, jsonify, request
import subprocess
import os
from datetime import datetime

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "windborne-etl-api"
    }), 200

@app.route('/run-etl', methods=['POST'])
def run_etl():
    """Execute ETL pipeline"""
    try:
        print(f"[{datetime.now()}] Starting ETL execution...")
        
        # Executar main.py
        result = subprocess.run(
            ['python', 'main.py'],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )
        
        response = {
            "status": "success" if result.returncode == 0 else "error",
            "timestamp": datetime.now().isoformat(),
            "returncode": result.returncode,
            "stdout": result.stdout[-1000:] if result.stdout else "",  # últimos 1000 chars
            "stderr": result.stderr[-1000:] if result.stderr else ""
        }
        
        status_code = 200 if result.returncode == 0 else 500
        
        print(f"[{datetime.now()}] ETL finished with status: {response['status']}")
        
        return jsonify(response), status_code
        
    except subprocess.TimeoutExpired:
        return jsonify({
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "message": "ETL execution timeout (5 minutes)"
        }), 500
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "message": str(e)
        }), 500

@app.route('/status', methods=['GET'])
def status():
    """Get last ETL run status from database"""
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', 5432),
            database=os.getenv('POSTGRES_DB', 'windborne_finance'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                run_date,
                workflow_name,
                companies_processed,
                api_calls_made,
                execution_time_seconds,
                status
            FROM etl_runs
            ORDER BY run_date DESC
            LIMIT 1
        """)
        
        row = cur.fetchone()
        
        if row:
            return jsonify({
                "last_run": {
                    "date": row[0].isoformat(),
                    "workflow": row[1],
                    "companies": row[2],
                    "api_calls": row[3],
                    "duration": row[4],
                    "status": row[5]
                }
            }), 200
        else:
            return jsonify({"message": "No ETL runs found"}), 404
            
        cur.close()
        conn.close()
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    print("🚀 WindBorne ETL API Starting...")
    print(f"📡 Listening on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
