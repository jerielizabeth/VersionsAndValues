import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def check_if_new_repos(repo_df):
    query = f"https://api.github.com/search/repositories?q=topic:digital-humanities&per_page=100&page=1"
    try:
        response = requests.get(query, headers=auth_headers)
        response_data = response.json()
        updated_count = response_data['total_count']
        if updated_count > repo_df.shape[0]:    
            return True
    except:
        print(f"Error on checking for new repos")
        return False

def get_repos_data(output_path):
    dfs = []
    for i in tqdm(range(1,7), desc="Getting Repos"):
        query = f"https://api.github.com/search/repositories?q=topic:digital-humanities&per_page=100&page={i}"
        try:
            response = requests.get(query, headers=auth_headers)
            response_data = response.json()
            response_df = pd.DataFrame.from_dict(response_data['items'])
            response_df['query'] = query
            dfs.append(response_df)
        except:
            print(f"Error on page {i}")
    repo_df = pd.concat(dfs)
    repo_df = repo_df.reset_index(drop=True)
    repo_df.to_csv(output_path, index=False)
    return repo_df

def get_all_repos_topic_dh(output_path):
    if os.path.exists(output_path):
        repo_df = pd.read_csv(output_path)
        new_repos = check_if_new_repos(repo_df)
        if new_repos:
            os.remove(output_path) #could update this to actually update original file but going to use this for now
            repo_df = get_repos_data(output_path)
    else:
        repo_df = get_repos_data(output_path)
        
    return repo_df

def get_languages(row):
    response = requests.get(row.languages_url, headers=auth_headers)
    return response.json()

def get_repo_languages(repo_df, output_path):
    if os.path.exists(output_path):
        repo_df = pd.read_csv(output_path)
    else:
        tqdm.pandas(desc="Getting Languages")
        repo_df['languages'] = repo_df.progress_apply(get_languages, axis=1)
        repo_df.to_csv(output_path, index=False)
    return repo_df

def get_contributors(repo_df, output_path):
    contributors_rows = []
    for _, row in tqdm(repo_df.iterrows(), total=repo_df.shape[0], desc="Getting Contributors"):
        try: 
            url = row.contributors_url
            response = requests.get(url, headers=auth_headers)
            response_data = response.json()
            df = pd.json_normalize(response_data)
            df['repo_id'] = row.id
            df['html_url'] = row.html_url
            df['full_name'] = row.full_name
            contributors_rows.append(df)
        except:
            print(f"Error on getting contributors for {row.full_name}")
            continue
    contributors_df = pd.concat(contributors_rows)
    contributors_df.to_csv(output_path, index=False)
    return contributors_df

def get_repo_contributors(repo_df, output_path):
    if os.path.exists(output_path):
        contributors_df = pd.read_csv(output_path)
        if len(contributors_df[contributors_df.login.isna()]) > 0:
            existing_contributors = contributors_df[contributors_df.login.isna() == False]
            missing_repos = contributors_df[contributors_df.login.isna()].html_url.unique().tolist()
            missing_repos_df = repo_df[repo_df.html_url.isin(missing_repos)]
            missing_repos_df = get_contributors(missing_repos_df, output_path)
            contributors_df = pd.concat([existing_contributors, missing_repos_df])
            contributors_df.to_csv(output_path, index=False)
    else:
        contributors_df = get_contributors(repo_df, output_path)
    return contributors_df

if __name__ == "__main__":
    repo_df = get_all_repos_topic_dh('../data/repos_topic_dh.csv')
    repo_languages_df = get_repo_languages(repo_df, '../data/repos_topic_dh_languages.csv')
    contributors_df = get_repo_contributors(repo_df, '../data/repos_topic_dh_contributors.csv')