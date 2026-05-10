import sys
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_env_file():
    env_path = ROOT / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()

from crew import run_personalization, run_merchandising, run_full_crew

def main():
    print("\nShoe Personalization System - Test Mode")
    print("=" * 50)
    print("1. Personalization Agent (recommend for one user)")
    print("2. Merchandising Agent (what to promote)")
    print("3. Full Crew (both agents)")
    print("=" * 50)

    choice = input("\nEnter 1, 2 or 3: ").strip()

    if choice == "1":
        userid = input("Enter userid (1-100): ").strip()
        result = run_personalization(int(userid))
        print("\nRECOMMENDATION:")
        print(result)

    elif choice == "2":
        result = run_merchandising()
        print("\nMERCHANDISING RECOMMENDATIONS:")
        print(result)

    elif choice == "3":
        userid = input("Enter userid (1-100): ").strip()
        result = run_full_crew(int(userid))
        print("\nFULL CREW OUTPUT:")
        print(result)

    else:
        print("Invalid choice")
        sys.exit(1)

if __name__ == "__main__":
    main()
