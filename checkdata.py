import psycopg2

conn = psycopg2.connect(
    dbname="boards", user="postgres", password="password", host="localhost"
)
cursor = conn.cursor()

# cursor.execute("SELECT link FROM image_cache LIMIT 10;")

# Get all links from the table
rows = cursor.fetchall()


# Print catbox links
# Print only Catbox links
# for (link,) in rows:
#     if "catbox" in link:
#         print(link)


# Check if a particular image exists in db
hash = "7df49a3136314917335c0921fcecac20"

conn = psycopg2.connect(
    dbname="boards", user="postgres", password="password", host="localhost"
)
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM image_cache WHERE hash = %s LIMIT 1;", (hash,))
    exists = cur.fetchone() is not None
conn.close()
print(exists)
cursor.close()
conn.close()


# import requests

# response = requests.post(
#     "https://api.imgur.com/oauth2/token",
#     data={
#         "refresh_token": "YOUR_REFRESH_TOKEN",
#         "client_id": "YOUR_CLIENT_ID",
#         "client_secret": "YOUR_CLIENT_SECRET",
#         "grant_type": "refresh_token",
#     },
# )

# print(response.json())
