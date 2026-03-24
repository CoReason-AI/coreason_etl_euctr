from coreason_etl_euctr.bronze_loader import EpistemicBronzeLoaderTask

def check_bronze_data():
    loader = EpistemicBronzeLoaderTask()
    # This reads the coreason_etl_euctr.duckdb file
    all_blobs = loader.read_all_html_blobs()
    
    total_trials = len(all_blobs)
    print(f"\n--- Bronze Layer Verification ---")
    print(f"Total unique trials stored: {total_trials}")
    
    # Count the total individual HTML files (GB, DE, BE, 3rd)
    total_files = sum(len(geographies) for geographies in all_blobs.values())
    print(f"Total raw HTML files stored: {total_files}")
    
    # Show a sample
    if all_blobs:
        sample_id = list(all_blobs.keys())[0]
        geos = list(all_blobs[sample_id].keys())
        print(f"Sample Trial {sample_id} contains HTML for: {geos}")

if __name__ == "__main__":
    check_bronze_data()
