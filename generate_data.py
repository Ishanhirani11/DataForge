import csv, random
rows = [["id", "name", "email", "age", "country"]]
countries = ["US", "UK", "CA", "AU", "IN"] 
for i in range(1, 1001):
    rows.append([i, f"User{i}", f"user{i}@example.com", random.randint(18, 80), random.choice(countries)])
with open("data/sample_1000.csv", "w", newline="") as f:
    csv.writer(f).writerows(rows)
print("Created data/sample_1000.csv with 1000 rows")