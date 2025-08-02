import os

# Create folder structure
base_path = "F:/PvD/Studying/Programming/DataProcessing/TimoTest"
folders = [
    "sql",
    "src",
    "dags_or_jobs",
    "visualization"
]

for folder in folders:
    os.makedirs(os.path.join(base_path, folder), exist_ok=True)

# Create README.md
readme_path = os.path.join(base_path, "README.md")
with open(readme_path, "w") as f:
    f.write("# Banking Data Assignment\n\n## Project Overview\nThis project implements a secure, compliant data platform for a simplified banking system.\n")

base_path
