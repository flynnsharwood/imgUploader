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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.image_cache (
            hash TEXT PRIMARY KEY,
            link TEXT NOT NULL,
            filename TEXT,
            uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
    cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS imgchest_posts (
        post_id TEXT PRIMARY KEY,
        folder_path TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        image_count INTEGER NOT NULL,
        uploaded_at TIMESTAMPTZ DEFAULT NOW()
    )
    """
)

    # cursor.execute("""
    #     ALTER TABLE public.image_cache
    #     ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    # """)

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_image_cache_filename ON public.image_cache(filename)"
    )



def save_link(cur, hash_val, link, filename_or_none):
    """
    Upsert link by hash. Preserve existing filename; only set it if currently NULL
    and we are allowed to supply one (i.e., not a duplicate filename case).
    """
    cur.execute(
        """
        INSERT INTO public.image_cache
            (hash, link, filename, uploaded_at)
        VALUES
            (%s, %s, %s, NOW())
        ON CONFLICT (hash) DO UPDATE
        SET
            link = EXCLUDED.link,
            filename = COALESCE(
                public.image_cache.filename,
                EXCLUDED.filename
            )
        """,
        (hash_val, link, filename_or_none),
    )


def save_imgchest_post(
    cur,
    folder_path,
    post_id,
    sequence,
    image_count
):
    cur.execute(
        """
        INSERT INTO imgchest_posts
        (
            post_id,
            folder_path,
            sequence,
            image_count
        )
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (post_id)
        DO NOTHING
        """,
        (
            post_id,
            folder_path,
            sequence,
            image_count
        )
    )
