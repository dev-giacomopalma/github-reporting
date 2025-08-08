import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import math
import time

# GitHub authentication token
GITHUB_TOKEN = "<GITHUB TOKEN>"
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"}

# Fixed organization name
ORG_NAME = "<GITHUB ORGANIZATION>"

# Placeholder for a constant list of usernames
DEFAULT_USERNAMES = [
    "<USERNAME 1>",
    "<USERNAME 2>"
]

# Wait time in seconds between API calls to avoid rate limits
DEFAULT_WAIT_TIME = 2
EXTENDED_WAIT_TIME = 60  # Extended wait time for handling rate limit errors
MAX_RETRIES = 5  # Maximum number of retries for a single request

def make_api_request(url):
    """
    Make an API request with retry logic to handle rate limits.
    """
    retries = 0
    while retries < MAX_RETRIES:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            return response
        elif response.status_code == 403 and "secondary rate limit" in response.text.lower():
            print(f"Rate limit exceeded. Waiting {EXTENDED_WAIT_TIME} seconds before retrying...")
            time.sleep(EXTENDED_WAIT_TIME)  # Wait before retrying
            retries += 1
        else:
            print(f"Error: {response.status_code} - {response.text}")
            if retries < MAX_RETRIES - 1:
                print(f"Retrying in {DEFAULT_WAIT_TIME} seconds...")
                time.sleep(DEFAULT_WAIT_TIME)  # Wait before retrying
                retries += 1
            else:
                print("Max retries reached. Skipping this request.")
                return None
    return None

# Fetch PRs for a user with pagination
def fetch_user_prs(username, months=None):
    """
    Fetch pull requests created by a specific user in the organization.
    If months is not provided, fetch data starting from January 1st of the current year.
    """
    if months:
        start_date = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")
    else:
        start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")

    print(f"Fetching PRs for user '{username}' starting from: {start_date}")
    base_url = f"https://api.github.com/search/issues?q=author:{username}+org:{ORG_NAME}+type:pr+created:>={start_date}"
    prs = []
    url = base_url

    while url:
        print(f"Querying: {url}")
        response = make_api_request(url)
        if response and response.status_code == 200:
            data = response.json()
            prs.extend(data.get('items', []))

            url = None
            if 'Link' in response.headers:
                links = response.headers['Link'].split(', ')
                for link in links:
                    if 'rel="next"' in link:
                        url = link.split('; ')[0].strip('<>')
        elif response is None:
            print("Skipping this request due to repeated errors.")
            break

    print(f"Total PRs fetched for user '{username}': {len(prs)}")
    return prs

# Fetch full PR details using the Pulls API
def fetch_full_pr_details(pr):
    """
    Fetch full details for a pull request using the Pulls API.
    """
    url = pr['pull_request']['url']  # Use the URL provided in the Search API response
    response = make_api_request(url)
    if response and response.status_code == 200:
        pr_details = response.json()
        # Only return PRs that are merged
        if pr_details.get('merged_at'):
            return pr_details
        else:
            print(f"Skipping PR {pr_details['html_url']} because it is not merged.")
            return None
    else:
        print(f"Error fetching full details for PR {pr['html_url']}.")
        return None

# Fetch files for PRs
def fetch_files_for_pr(pr):
    """
    Fetch file details for a pull request.
    """
    if not pr or '_links' not in pr:
        return []  # Return empty list if PR details are missing or invalid
    url = pr['_links']['self']['href'] + '/files'
    response = make_api_request(url)
    if response and response.status_code == 200:
        files = response.json()
        return files
    else:
        print(f"Error fetching files for PR {pr['html_url']}.")
        return []

# Check if PR was merged without approval
def check_merged_without_approval(pr):
    """
    Check if a pull request was merged without approval.
    Returns 1 if merged without approval, otherwise 0.
    """
    if not pr or '_links' not in pr:
        return 1  # Default to 1 if PR details are missing
    url = pr['_links']['self']['href'] + '/reviews'
    response = make_api_request(url)
    if response and response.status_code == 200:
        reviews = response.json()
        # Check if there are any approval reviews
        for review in reviews:
            if review['state'].lower() == 'approved':
                return 0  # PR was approved before merging
        return 1  # No approval found
    else:
        print(f"Error fetching reviews for PR {pr['html_url']}. Assuming merged without approval.")
        return 1

# Count non-autogenerated changes and lines of code in PRs
def count_changes_and_lines(prs):
    """
    Count the number of changes and lines of code for all PRs using parallel API calls.
    """
    print("Fetching file details for all PRs in parallel...")
    changes_and_lines = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Fetch full PR details for each PR
        full_pr_details = list(executor.map(fetch_full_pr_details, prs))
        # Filter out None (non-merged PRs)
        full_pr_details = [pr for pr in full_pr_details if pr is not None]
        # Fetch files for each PR
        file_results = executor.map(fetch_files_for_pr, full_pr_details)
        for pr, files in zip(full_pr_details, file_results):
            if pr and files:  # Ensure PR details and files are available
                non_autogenerated_files = [
                    f for f in files if not f['filename'].endswith(('.lock', '.json', '.md'))
                ]
                file_count = len(non_autogenerated_files)
                total_lines_changed = sum(f['changes'] for f in non_autogenerated_files)
                changes_and_lines.append((file_count, total_lines_changed))
            else:
                changes_and_lines.append((0, 0))  # Default values if PR or files are missing
    return changes_and_lines, full_pr_details

# Calculate merge time in hours
def calculate_merge_time(pr):
    """
    Calculate the time it took for the PR to be merged, in hours (rounded).
    """
    created_at = datetime.strptime(pr['created_at'], "%Y-%m-%dT%H:%M:%SZ")
    merged_at = datetime.strptime(pr['merged_at'], "%Y-%m-%dT%H:%M:%SZ")
    merge_time = (merged_at - created_at).total_seconds() / 3600
    return math.ceil(merge_time)

# Generate report for a user
def generate_report(username, months=None):
    """
    Generate a report for the given GitHub username in the organization.
    """
    print(f"Fetching merged PRs for user '{username}' in organization '{ORG_NAME}'...")
    prs = fetch_user_prs(username, months)
    if not prs:
        print(f"No PRs found for user '{username}'.")
        return None

    print("Counting non-autogenerated changes and lines of code...")
    changes_and_lines, full_pr_details = count_changes_and_lines(prs)

    print("Calculating merge times and approvals...")
    merge_times = [calculate_merge_time(pr) for pr in full_pr_details]
    merged_without_approvals = [check_merged_without_approval(pr) for pr in full_pr_details]

    report_data = []
    for pr, changes, merge_time, no_approval in zip(full_pr_details, changes_and_lines, merge_times, merged_without_approvals):
        # Extract date (YYYY-MM-DD) from created_at
        created_date = datetime.strptime(pr['created_at'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
        report_data.append({
            "PR URL": pr['html_url'],
            "PR Created Date": created_date,  # Use only the date part
            "Merge Time (hours)": merge_time,
            "Merged Without Approval": no_approval,
            "Non-Autogenerated Changes": changes[0],
            "Lines of Code Changed": changes[1]
        })

    return pd.DataFrame(report_data)

def generate_reports_for_users(usernames, months=None):
    """
    Generate separate reports for each user and save them as CSV files.
    """
    for username in usernames:
        print(f"Processing user: {username}")
        report_df = generate_report(username, months)
        if report_df is not None:
            # Save individual report for the user
            filename = f"{username}_pr_report.csv"
            report_df.to_csv(filename, index=False)
            print(f"Report saved as '{filename}'.")

# Run script
if __name__ == "__main__":
    print(f"Organization: {ORG_NAME}")

    # Prompt for username
    github_username = input("Enter GitHub username (leave blank to use default list): ")
    months_input = input("Enter how many months of data you want to export (leave blank for default): ")
    months = int(months_input) if months_input else None

    # Determine usernames to process
    if github_username.strip():
        usernames = [github_username]
    else:
        usernames = DEFAULT_USERNAMES

    # Generate reports
    generate_reports_for_users(usernames, months)
