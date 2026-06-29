CREATE TABLE IF NOT EXISTS imgchest_posts (
    post_id TEXT PRIMARY KEY,
    folder_path TEXT NOT NULL,
    image_count INTEGER NOT NULL,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gallery_posts (
    folder_path TEXT NOT NULL,
    post_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    PRIMARY KEY(folder_path, post_id)
);
