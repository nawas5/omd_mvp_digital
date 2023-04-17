from sqlalchemy import create_engine

"""
подключение к базе
"postgresql://olv_master:xSxuQ{pC\_a6:S#p@172.17.2.55:5432/olv_master_base"
"""

engine_url = "postgresql://olv_master:xSxuQ{pC\_a6:S#p@172.17.2.55:5432/olv_master_base"
db = create_engine(engine_url)

try:
    conn = db.connect()
    print("Соединение с базой данных установлено.")
except Exception as e:
    print("Не удалось установить соединение с базой данных:", e)



db = create_engine(engine_url)
db.execute("""
    CREATE TABLE digital_rus (
        "key_id" text NOT NULL,
        "adv_list_rus" text NOT NULL,
        "art3_list_rus" text NOT NULL,
        "ad_placement" text,
        "use_type" text NOT NULL,
        "subbr_list_rus" text NOT NULL,
        "day" date NOT NULL,
        "week" date NOT NULL,
        "month" date NOT NULL,
        "ad_network" text,
        "br_list_rus" text NOT NULL,
        "mod_list_rus" text NOT NULL,
        "media_product" text NOT NULL,
        "ad_source_type" text NOT NULL,
        "art2_list_rus" text NOT NULL,
        "id" integer NOT NULL,
        "ad_player" text,
        "media_resource" text NOT NULL,
        "ad_server" text,
        "media_holding" text NOT NULL,
        "first_issue_date" date,
        "art4_list_rus" text NOT NULL,
        "ad_video_utility" text NOT NULL,
        "ots" integer NOT NULL,
        PRIMARY KEY ("key_id")
    );
""")

