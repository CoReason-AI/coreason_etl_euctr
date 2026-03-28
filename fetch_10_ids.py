import re
import urllib.request


def get_10_euctr_ids() -> None:
    ids: set[str] = set()
    page = 1
    # EudraCT Number format regex (e.g., 2025-000181-28)
    pattern = re.compile(r"\b\d{4}-\d{6}-\d{2}\b")

    print("Fetching 10 recent EudraCT IDs...")
    while len(ids) < 10:
        # EudraCT Number format regex (e.g., 2025-000181-28)
        url = f"https://www.clinicaltrialsregister.eu/ctr-search/search?query=&page={page}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})  # noqa: S310
        try:
            with urllib.request.urlopen(req) as response:  # noqa: S310
                html = response.read().decode("utf-8")
                found = pattern.findall(html)
                for fid in found:
                    if len(ids) < 10:
                        ids.add(fid)
            page += 1
        except Exception as e:
            print(f"Error fetching page {page}: {e}")
            break

    with open("test_10_ids.txt", "w") as f:
        f.writelines(f"{trial_id}\n" for trial_id in sorted(ids, reverse=True))

    print(f"Successfully saved {len(ids)} IDs to test_10_ids.txt")


if __name__ == "__main__":
    get_10_euctr_ids()
