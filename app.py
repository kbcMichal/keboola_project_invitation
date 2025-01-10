import streamlit as st
import csv
import requests
from filelock import FileLock

# Constants
PROJECTS_FILE = "projects.csv"
EMAILS_FILE = "emails.csv"
API_BASE_URL = "https://connection.keboola.com/manage/projects/{project_id}/invitations"
TOKEN = st.secrets['MANAGE_TOKEN']
HEADERS = {
    "X-KBC-ManageApiToken": TOKEN
}

# Load CSV Data
def load_csv(file_path):
    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

# Save CSV Data
def save_csv(file_path, data, fieldnames):
    with open(file_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Check if email is already invited
def is_email_invited(email, emails):
    for entry in emails:
        if entry["email"] == email and entry["project_id"]:
            return True
    return False

# Get the next available project
def get_next_project(projects, emails):
    used_project_ids = {entry["project_id"] for entry in emails if entry["project_id"]}
    for project in projects:
        if project["id"] not in used_project_ids:
            return project["id"]
    return None

# Invite User via API
def invite_user(email, project_id):
    url = API_BASE_URL.format(project_id=project_id)
    payload = {
        "email": email,
        "role": "admin",
        "expirationSeconds": 1210000,
    }
    response = requests.post(url, json=payload, headers=HEADERS)
    return response.status_code, response.text  # Return both for debugging

# Main Streamlit App
def main():
    st.title("Project Invitation System")
    email = st.text_input("Enter your email address:")
    submit = st.button("Submit")

    if submit:
        if not email:
            st.error("Please enter a valid email address.")
            return

        with FileLock(EMAILS_FILE + ".lock"):
            # Load projects and emails
            projects = load_csv(PROJECTS_FILE)
            emails = load_csv(EMAILS_FILE)

            # Check if the email has already been invited
            if is_email_invited(email, emails):
                st.error("This email has already been invited to a project.")
                return

            # Get the next available project
            project_id = get_next_project(projects, emails)
            if not project_id:
                st.error("No projects available for invitations.")
                return

            # Invite the user
            status_code, response = invite_user(email, project_id)

            # Display debugging info
            st.text(f"Response Status Code: {status_code}")
            st.text_area("Response Body", response)

            if status_code in {200, 201, 204}:  # Success statuses
                st.success("Invited! Check your mailbox.")
                # Add the email and project_id to the emails list
                emails.append({"email": email, "project_id": project_id})
                save_csv(EMAILS_FILE, emails, ["email", "project_id"])
            else:
                st.error("Failed to invite. Please check the debug info above.")

if __name__ == "__main__":
    main()