import psycopg2


def connect_aiven():
    return psycopg2.connect(
        host=aiven_host,
        port="...",
        dbname="defaultdb",
        user="avnadmin",
        password="...",
        sslmode="require",
    )

def connect_db():
    return psycopg2.connect(
        dbname="boards",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )


def create_table_if_not_exists(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS public.image_cache (
            hash TEXT PRIMARY KEY,
            link TEXT NOT NULL,
            filename TEXT,
            uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    # cursor.execute("""
    #     ALTER TABLE public.image_cache
    #     ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    # """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_image_cache_filename ON public.image_cache(filename)")

    