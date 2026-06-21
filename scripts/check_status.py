import psycopg2

try:
    # 1. Connect to Airflow database
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        database="airflow"
    )
    cursor = conn.cursor()
    cursor.execute("""
        select task_id, state, start_date, end_date 
        from task_instance 
        where dag_id='batch_etl_pipeline' 
          and run_id='manual__2026-06-19T00:00:00+00:00' 
        order by start_date;
    """)
    rows = cursor.fetchall()
    print("Airflow Task States:")
    print("--------------------------------------------------------------------------------")
    for row in rows:
        print(f"Task: {row[0]:<20} | State: {str(row[1]):<10} | Start: {str(row[2]):<25} | End: {str(row[3])}")
    print("--------------------------------------------------------------------------------")
    cursor.close()
    conn.close()
    
    # 2. Connect to Warehouse database
    conn_wh = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        database="warehouse"
    )
    cursor_wh = conn_wh.cursor()
    
    print("\nWarehouse Data Counts:")
    for table in ["daily_active_users", "product_sales", "funnel_metrics"]:
        try:
            cursor_wh.execute(f"select count(*) from {table};")
            count = cursor_wh.fetchone()[0]
            print(f"Table: {table:<20} | Rows: {count}")
        except Exception as e:
            print(f"Table: {table:<20} | Error: {e}")
            conn_wh.rollback()
            
    # Print sample data from daily_active_users
    try:
        cursor_wh.execute("select * from daily_active_users limit 5;")
        dau_rows = cursor_wh.fetchall()
        if dau_rows:
            print("\nSample daily_active_users:")
            for r in dau_rows:
                print(r)
    except Exception as e:
        conn_wh.rollback()
        
    cursor_wh.close()
    conn_wh.close()
            
except Exception as e:
    print(f"Error querying database: {e}")
