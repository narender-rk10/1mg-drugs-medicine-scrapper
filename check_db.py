import sqlite3
conn = sqlite3.connect("1mg_medicines.db")
total = conn.execute("SELECT COUNT(*) FROM medicines").fetchone()[0]
failed = conn.execute("SELECT COUNT(*) FROM failed_slugs").fetchone()[0]
labels = conn.execute(
    "SELECT SUBSTR(slug, 8, 1) as lbl, COUNT(*) FROM medicines GROUP BY lbl ORDER BY lbl"
).fetchall()
pages = conn.execute(
    "SELECT label, COUNT(*) FROM scrape_progress WHERE status IN ('done','listing_done') GROUP BY label ORDER BY label"
).fetchall()
print(f"Medicines stored:  {total}")
print(f"Failed slugs:      {failed}")
print("By label:")
for lbl, cnt in labels:
    print(f"  {lbl}: {cnt}")
print("Listing pages done:")
for lbl, cnt in pages:
    print(f"  {lbl}: {cnt}")
conn.close()
