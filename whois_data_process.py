import csv
import os

def parse_unzipped_inetnum(file_path, output_csv):
    """
    Parses a large unzipped RIPE inetnum file line-by-line.
    """
    print(f"Opening {file_path}... This may take a moment.")
    
    # Check file size to give you a heads up
    file_size_gb = os.path.getsize(file_path) / (1024**3)
    print(f"File size: {file_size_gb:.2f} GB")

    with open(file_path, 'r', encoding='latin-1') as f, \
         open(output_csv, 'w', newline='') as out:
        
        writer = csv.writer(out)
        writer.writerow(['inetnum', 'org_id', 'netname', 'created_date', 'last_modified'])

        current_record = {}
        count = 0
        leases_found = 0

        for line in f:
            line = line.strip()

            # End of an object block is a blank line
            if not line:
                if 'inetnum' in current_record:
                    # Logic: Extracting the fields for your mismatch engine
                    writer.writerow([
                        current_record.get('inetnum', ''),
                        current_record.get('org', ''),
                        current_record.get('netname', ''),
                        current_record.get('created', ''),
                        current_record.get('last-modified', '')
                    ])
                    leases_found += 1
                
                current_record = {}
                continue

            # Skip comments
            if line.startswith('%'):
                continue

            # Parse Key: Value
            if ':' in line:
                parts = line.split(':', 1)
                key = parts[0].strip().lower()
                value = parts[1].strip()

                # These are the specific fields the IMC paper uses
                if key in ['inetnum', 'org', 'netname', 'created', 'last-modified']:
                    current_record[key] = value

            # Progress tracker
            count += 1
            if count % 1000000 == 0:
                print(f"Read {count} lines... Records saved: {leases_found}")

    print(f"\nSuccess! Total records saved to {output_csv}: {leases_found}")

# --- RUN IT ---
# Replace 'ripe.db.inetnum' with your actual filename if it's different
parse_unzipped_inetnum('ripe.db.inetnum', 'ripe_processed_owners.csv')