import sys
from crew import run_personalization, run_merchandising, run_full_crew

def main():
    print("\n🥾 Shoe Personalization System - Test Mode")
    print("=" * 50)
    print("1. Personalization Agent (recommend for one user)")
    print("2. Merchandising Agent (what to promote)")
    print("3. Full Crew (both agents)")
    print("=" * 50)

    choice = input("\nEnter 1, 2 or 3: ").strip()

    if choice == "1":
        userid = input("Enter userid (1-100): ").strip()
        result = run_personalization(int(userid))
        print("\n✅ RECOMMENDATION:")
        print(result)

    elif choice == "2":
        result = run_merchandising()
        print("\n✅ MERCHANDISING RECOMMENDATIONS:")
        print(result)

    elif choice == "3":
        userid = input("Enter userid (1-100): ").strip()
        result = run_full_crew(int(userid))
        print("\n✅ FULL CREW OUTPUT:")
        print(result)

    else:
        print("Invalid choice")
        sys.exit(1)

if __name__ == "__main__":
    main()