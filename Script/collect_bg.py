import pandas as pd
import requests
import time
from tqdm import *
# GitHub API token for authentication (replace with your token)
GITHUB_TOKEN = ''
HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

# Load the CSV file containing Commit-IDs
file_path = 'domain_stat.csv'
df = pd.read_csv(file_path)

# Add columns for commit, user, and repository details
df['Author'] = None
df['Repository_Content'] = None
df['Public_Repos'] = None
df['Public_Gists'] = None
df['Followers'] = None
df['Following'] = None
df['Stargazers_Count'] = None
df['Watchers_Count'] = None
df['Network_Count'] = None
df['Subscribers_Count'] = None
df['Forks'] = None

# Function to fetch commit details using Search API
def fetch_commit_details_via_search(commit_id):
    try:
        url = f"https://api.github.com/search/commits?q=hash:{commit_id}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        if data.get('total_count', 0) > 0:
            commit = data['items'][0]
            author_info = commit.get('commit', {}).get('author', {}).get('login', 'Unknown')
            repo_full_name = commit.get('repository', {}).get('full_name')
            repo_content = commit.get('commit', {}).get('message', 'No message provided')
            return author_info, repo_content
        else:
            return None, None
    except Exception as e:
        print(f"Error fetching commit {commit_id}: {e}")
        return None, None


# Function to fetch user information
def fetch_user_info(username):
    try:
        url = f"https://api.github.com/users/{username}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        user_data = response.json()

        public_repos = user_data.get("public_repos", "N/A")
        public_gists = user_data.get("public_gists", "N/A")
        followers = user_data.get("followers", "N/A")
        following = user_data.get("following", "N/A")

        return public_repos, public_gists, followers, following
    except Exception as e:
        print(f"Error fetching user data for {username}: {e}")
        return "N/A", "N/A", "N/A", "N/A"

# Process Commit-IDs to fetch details
for index, row in df.iterrows():
    commit_id = row['sha']
    author, content = fetch_commit_details_via_search(commit_id)
    author=row['username']
    df.at[index, 'username'] = author
    df.at[index, 'Repository_Content'] = content

    # Fetch user statistics if author is available
    if author and author != "Unknown":
        public_repos, public_gists, followers, following = fetch_user_info(author)
        df.at[index, 'Public_Repos'] = public_repos
        df.at[index, 'Public_Gists'] = public_gists
        df.at[index, 'Followers'] = followers
        df.at[index, 'Following'] = following

    # Avoid hitting rate limits
    time.sleep(1)

# Save the updated CSV
output_path = 'background_details.csv'
df.to_csv(output_path, index=False)
print(f"Updated CSV saved to {output_path}")

