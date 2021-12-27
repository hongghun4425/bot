import pymysql
from library import cf

conn = pymysql.connect(
    host=cf.db_ip,
    user=cf.db_id,
    password=cf.db_passwd,
    port=int(cf.db_port),
    cursorclass=pymysql.cursors.DictCursor
)


def get_create_index_sql(shema_name, table_name, col_name, index_name):
    if col_name == 'date' and shema_name == 'min_craw':
        fixed_col_name = 'date(12)'
    elif col_name == 'date' and shema_name == 'daily_craw':
        fixed_col_name = 'date(8)'
    elif col_name == 'code':
        fixed_col_name = 'code(6)'

    return f"""\
        CREATE INDEX {index_name}
        ON `{shema_name}`.`{table_name}` ({fixed_col_name})
    """


cursor = conn.cursor()

table_names_sql = """
    SELECT table_schema as db, table_name as table_name FROM information_schema.tables
    WHERE table_schema in ('daily_craw', 'min_craw', 'daily_buy_list')
"""

cursor.execute("""
    SELECT DISTINCT
        INDEX_NAME
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA in ('daily_buy_list', 'daily_craw', 'min_craw')
""")
existing_indexes = [item['INDEX_NAME'] for item in cursor.fetchall()]

cursor.execute(table_names_sql)
table_names = cursor.fetchall()
total_count = len(table_names)
for i, row in enumerate(table_names):
    if row['db'] == 'daily_buy_list':
        col_name = 'code'
    elif row['db'] in ('daily_craw', 'min_craw'):
        col_name = 'date'
    index_name = f"ix_{''.join(c for c in row['table_name'] if c.isalnum())}_{col_name}"
    progress = ((i + 1) * 100) / total_count
    print(f"""\r{progress:>8.2f}% [{'=' * int(progress // 5):<20}] {row['db']}.{row['table_name']}""", end='')
    if index_name in existing_indexes:
        continue
    sql = get_create_index_sql(row['db'], row['table_name'], col_name, index_name)

    try:
        cursor.execute(sql)
    except (pymysql.err.InternalError, pymysql.err.OperationalError) as e:
        pass

cursor.close()
