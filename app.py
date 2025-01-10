import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import requests
import json


TOKEN = st.secrets['MANAGE_TOKEN']
HEADERS = {
    "X-KBC-ManageApiToken": TOKEN
}

# Snowflake configuration
def create_snowflake_session():
    # Replace with your Snowflake credentials
    connection_parameters = {
        "account": st.secrets['ACCOUNT'],
        "user": st.secrets['USER'],
        "password": st.secrets['PASSWORD'],
        "warehouse": st.secrets['WAREHOUSE'],
        "database": st.secrets['DATABASE'],
        "schema": st.secrets['SCHEMA']
    }
    return Session.builder.configs(connection_parameters).create()

# Load projects and emails from Snowflake
def load_projects(session):
    return session.table("PROJECTS").to_pandas()

def load_emails(session):
    return session.table("EMAILS").to_pandas()

# Check if an email is already invited
def is_email_invited(email, emails_df):
    return not emails_df[emails_df["EMAIL"] == email].empty

# Get the next available project
def get_next_project(projects_df, emails_df):
    used_project_ids = set(emails_df["PROJECT_ID"].dropna())
    available_projects = projects_df[~projects_df["ID"].isin(used_project_ids)]
    return available_projects.iloc[0]["ID"] if not available_projects.empty else None

# Insert a new email invitation into the emails table
def insert_email_invitation(session, email, project_id):
    session.sql(f"""
        INSERT INTO EMAILS (EMAIL, PROJECT_ID)
        VALUES ('{email}', {project_id})
    """).collect()

# API call to invite user to a project
def invite_user_to_project(project_id, email):
    url = f"https://connection.keboola.com/manage/projects/{project_id}/invitations"
    payload = {
        "email": email,
        "role": "admin",
        "expirationSeconds": 1210000
    }
    headers = HEADERS
    response = requests.post(url, json=payload, headers=headers)
    return response.status_code, response.json() if response.status_code != 204 else {}

# Main Streamlit app
def main():
    st.title("Project Invitation System")

    # Snowflake session
    session = create_snowflake_session()

    # User input
    email = st.text_input("Enter your email address:")
    submit = st.button("Submit")

    if submit:
        if not email:
            st.error("Please enter a valid email address.")
            return

        # Load data from Snowflake
        projects_df = load_projects(session)
        emails_df = load_emails(session)

        # Check if the email is already invited
        if is_email_invited(email, emails_df):
            st.error("This email has already been invited to a project.")
            return

        # Get the next available project
        project_id = get_next_project(projects_df, emails_df)
        if not project_id:
            st.error("No projects available for invitations.")
            return

        # Make the API call to invite the user
        status_code, response = invite_user_to_project(project_id, email)

        if status_code in {200, 201, 204}:  # Handle successful responses
            # Insert the invitation record into Snowflake
            insert_email_invitation(session, email, project_id)
            st.success(f"Invited! Check your mailbox. You are assigned to project ID: {project_id}")
        else:
            st.error(f"Failed to invite. Error: {response}")

if __name__ == "__main__":
    main()