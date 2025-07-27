import json
import os
import sqlite3
import datetime # NEW: For timestamps
import time     # NEW: For sleep (optional, for testing shutdown)
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler # NEW: For scheduling

app = Flask(__name__)
DATABASE = 'leads.db'
SAMPLE_LEADS_FILE = os.path.join('data', 'sample_leads.json')

# Initialize the scheduler
scheduler = BackgroundScheduler()

def init_db():
    """Initializes the SQLite database."""
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leads (
                id TEXT PRIMARY KEY,
                company_name TEXT,
                website TEXT,
                contact_name TEXT,
                email TEXT UNIQUE,
                linkedin_profile TEXT,
                industry TEXT,
                role TEXT,
                company_size TEXT,
                location TEXT,
                status TEXT DEFAULT 'pending',
                sent_date TEXT -- NEW COLUMN: To store timestamp of last send/followup
            )
        ''')
        conn.commit()
    print("Database initialized or already exists.")

@app.route('/')
def home():
    return "Welcome to the AI-Powered Outreach Pipeline API!"

@app.route('/api/leads/ingest', methods=['POST'])
def ingest_leads():
    """
    Ingests lead data into the database.
    Expects a JSON array of lead objects.
    """
    leads_data = request.json
    if not isinstance(leads_data, list):
        return jsonify({"error": "Expected a JSON array of lead objects"}), 400

    inserted_count = 0
    skipped_count = 0
    errors = []

    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        for lead in leads_data:
            try:
                # Basic validation - ensure essential fields are present
                if not all(k in lead for k in ['id', 'email', 'company_name', 'contact_name']):
                    errors.append(f"Skipping lead due to missing essential fields: {lead.get('id', 'N/A')}")
                    skipped_count += 1
                    continue

                # Insert lead, including a NULL sent_date initially
                cursor.execute('''
                    INSERT OR IGNORE INTO leads (
                        id, company_name, website, contact_name, email,
                        linkedin_profile, industry, role, company_size, location, status, sent_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                ''', (
                    lead.get('id'), lead.get('company_name'), lead.get('website'),
                    lead.get('contact_name'), lead.get('email'), lead.get('linkedin_profile'),
                    lead.get('industry'), lead.get('role'), lead.get('company_size'),
                    lead.get('location'), lead.get('status', 'pending')
                ))
                if cursor.rowcount > 0: # Check if a new row was inserted (not ignored)
                    inserted_count += 1
                else:
                    errors.append(f"Lead with email '{lead.get('email')}' already exists or ID collision.")
                    skipped_count += 1

            except sqlite3.Error as e:
                errors.append(f"Database error for lead {lead.get('id', 'N/A')}: {e}")
                skipped_count += 1
            except Exception as e:
                errors.append(f"An unexpected error for lead {lead.get('id', 'N/A')}: {e}")
                skipped_count += 1

        conn.commit()

    return jsonify({
        "message": "Lead ingestion complete",
        "inserted": inserted_count,
        "skipped": skipped_count,
        "errors": errors
    }), 200

@app.route('/api/leads', methods=['GET'])
def get_leads():
    """Fetches all leads from the database."""
    with sqlite3.connect(DATABASE) as conn:
        conn.row_factory = sqlite3.Row # Allows access by column name
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM leads')
        leads = [dict(row) for row in cursor.fetchall()]
    return jsonify(leads), 200

@app.route('/api/outreach/send/<lead_id>', methods=['POST'])
def send_outreach(lead_id):
    """
    Simulates sending an outreach email for a specific lead.
    Records the sent_date.
    """
    with sqlite3.connect(DATABASE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM leads WHERE id = ?', (lead_id,))
        lead = cursor.fetchone()

        if not lead:
            return jsonify({"error": f"Lead with ID '{lead_id}' not found."}), 404

        if lead['status'] != 'pending':
            return jsonify({"message": f"Outreach for lead ID '{lead_id}' already '{lead['status']}'."}), 200

        # Simulate email content generation (personalization)
        email_subject = f"Quick question for {lead['contact_name']} at {lead['company_name']}"
        email_body = f"""
        Hi {lead['contact_name']},

        Hope you're having a great week!

        I was Browse {lead['company_name']}'s website ({lead['website']}) and was particularly interested in your work in the {lead['industry']} space, especially regarding {lead['role']}.

        At [Your Company/Project Name - e.g., AI Outreach Pipeline], we help companies like yours streamline their outreach. I thought a brief chat about how we could potentially help you with lead generation might be valuable.

        Would you be open to a quick 15-minute call sometime next week?

        Best regards,

        Junaid (AI Outreach Pipeline)
        """

        try:
            print(f"--- Simulating Email Send ---")
            print(f"To: {lead['email']}")
            print(f"Subject: {email_subject}")
            print(f"Body:\n{email_body}")
            print(f"-----------------------------")

            # Update lead status and set the sent_date
            current_time_iso = datetime.datetime.now().isoformat()
            cursor.execute('UPDATE leads SET status = ?, sent_date = ? WHERE id = ?',
                           ('sent', current_time_iso, lead_id))
            conn.commit()

            return jsonify({
                "message": f"Outreach email simulated and status updated for lead ID '{lead_id}'.",
                "status": "sent",
                "to": lead['email'],
                "subject": email_subject,
                "sent_date": current_time_iso # Include in response for verification
            }), 200

        except sqlite3.Error as e:
            return jsonify({"error": f"Database error updating status for lead {lead_id}: {e}"}), 500
        except Exception as e:
            return jsonify({"error": f"An unexpected error occurred during simulated send for lead {lead_id}: {e}"}), 500

@app.route('/api/outreach/track/<lead_id>/<action>', methods=['POST'])
def track_engagement(lead_id, action):
    """
    Simulates tracking various engagement actions for a lead.
    Actions can be 'opened', 'replied', 'bounced'.
    """
    valid_actions = ['opened', 'replied', 'bounced']
    if action not in valid_actions:
        return jsonify({"error": f"Invalid action. Must be one of: {', '.join(valid_actions)}"}), 400

    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT status FROM leads WHERE id = ?', (lead_id,))
        current_status_row = cursor.fetchone()

        if not current_status_row:
            return jsonify({"error": f"Lead with ID '{lead_id}' not found."}), 404

        current_status = current_status_row[0]

        # Logic for status transitions
        new_status = current_status
        message = f"Lead '{lead_id}' status remains '{current_status}' after '{action}' action."

        if action == 'opened' and current_status == 'sent':
            new_status = 'opened'
            message = f"Lead '{lead_id}' status updated to 'opened'."
        elif action == 'replied' and current_status in ['sent', 'opened', 'pending', 'followup_sent']: # Allow reply from various states
            new_status = 'replied'
            message = f"Lead '{lead_id}' status updated to 'replied'. Take action!"
        elif action == 'bounced' and current_status in ['sent', 'pending', 'followup_sent']:
            new_status = 'bounced'
            message = f"Lead '{lead_id}' status updated to 'bounced'. Remove from list."
        elif action == 'replied' and current_status == 'replied':
            message = f"Lead '{lead_id}' already replied. No change needed."
        else:
            message = f"Action '{action}' not applicable for current status '{current_status}' for lead '{lead_id}'. Status remains '{current_status}'."


        if new_status != current_status:
            cursor.execute('UPDATE leads SET status = ? WHERE id = ?', (new_status, lead_id))
            conn.commit()

        return jsonify({"message": message, "lead_id": lead_id, "new_status": new_status}), 200

@app.route('/api/outreach/followup/<lead_id>', methods=['POST'])
def send_followup(lead_id):
    """
    Simulates sending a follow-up email for a specific lead,
    only if their status is 'sent' or 'opened'.
    Also updates sent_date.
    """
    with sqlite3.connect(DATABASE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM leads WHERE id = ?', (lead_id,))
        lead = cursor.fetchone()

        if not lead:
            return jsonify({"error": f"Lead with ID '{lead_id}' not found."}), 404

        # Only send follow-up if status is 'sent' or 'opened'
        if lead['status'] not in ['sent', 'opened']:
            return jsonify({
                "message": f"Cannot send follow-up. Lead '{lead_id}' status is '{lead['status']}'.",
                "status": lead['status']
            }), 400

        # Simulate follow-up email content generation
        followup_subject = f"Following up: {lead['contact_name']} at {lead['company_name']}"
        followup_body = f"""
        Hi {lead['contact_name']},

        Just wanted to gently follow up on my previous email. I know you're busy, but I genuinely believe that our AI Outreach Pipeline could bring significant value to {lead['company_name']}.

        If now isn't the best time, perhaps you could suggest a better moment to connect?

        Looking forward to hearing from you.

        Best regards,

        Junaid (AI Outreach Pipeline)
        """

        try:
            print(f"--- Simulating Follow-up Email Send ---")
            print(f"To: {lead['email']}")
            print(f"Subject: {followup_subject}")
            print(f"Body:\n{followup_body}")
            print(f"--------------------------------------")

            # Update lead status to 'followup_sent' and update sent_date
            current_time_iso = datetime.datetime.now().isoformat()
            cursor.execute('UPDATE leads SET status = ?, sent_date = ? WHERE id = ?',
                           ('followup_sent', current_time_iso, lead_id))
            conn.commit()

            return jsonify({
                "message": f"Follow-up email simulated and status updated for lead ID '{lead_id}'.",
                "status": "followup_sent",
                "to": lead['email'],
                "subject": followup_subject,
                "sent_date": current_time_iso # Include in response for verification
            }), 200

        except sqlite3.Error as e:
            return jsonify({"error": f"Database error updating status for lead {lead_id}: {e}"}), 500
        except Exception as e:
            return jsonify({"error": f"An unexpected error occurred during simulated follow-up send for lead {lead_id}: {e}"}), 500


# NEW FUNCTION: Automated Follow-up Check
def automated_followup_check():
    """
    This function is run by the scheduler.
    It checks for leads that are 'sent' or 'opened' but haven't been followed up
    within a certain timeframe (e.g., 1 minute for testing).
    """
    print("\n--- Running Automated Follow-up Check ---")
    followup_threshold_seconds = 60 # FOR TESTING: 1 minute (60 seconds)
    # For a real application, you'd use something like datetime.timedelta(days=3)

    with sqlite3.connect(DATABASE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Fetch leads that are 'sent' or 'opened' and have a sent_date
        cursor.execute("SELECT * FROM leads WHERE status IN ('sent', 'opened') AND sent_date IS NOT NULL")
        leads_to_check = cursor.fetchall()

        for lead in leads_to_check:
            try:
                # Convert ISO format string back to datetime object
                last_sent_time = datetime.datetime.fromisoformat(lead['sent_date'])
                time_since_last_send = datetime.datetime.now() - last_sent_time

                # Check if enough time has passed for a follow-up
                if time_since_last_send.total_seconds() >= followup_threshold_seconds:
                    print(f"Attempting automated follow-up for lead: {lead['id']} ({lead['contact_name']})")
                    # IMPORTANT: We are calling the core logic directly here, not the Flask route.
                    # This avoids HTTP request overhead.
                    send_followup_internal(lead['id'], conn, cursor) # Pass conn and cursor
                else:
                    print(f"Lead {lead['id']} not yet due for follow-up. Time remaining: {followup_threshold_seconds - time_since_last_send.total_seconds():.0f}s")
            except Exception as e:
                print(f"Error processing lead {lead['id']} for automated follow-up: {e}")
    print("--- Automated Follow-up Check Complete ---\n")

# NEW HELPER FUNCTION: Internal send_followup logic
# We create a new internal function to avoid making an HTTP request from the scheduler.
# This function reuses the core logic of send_followup but takes existing conn/cursor.
def send_followup_internal(lead_id, conn, cursor):
    """
    Internal function to send a follow-up, used by the scheduler.
    Assumes conn and cursor are already provided.
    """
    cursor.execute('SELECT * FROM leads WHERE id = ?', (lead_id,))
    lead = cursor.fetchone()

    if not lead:
        print(f"Error: Lead with ID '{lead_id}' not found for internal follow-up.")
        return

    if lead['status'] not in ['sent', 'opened']:
        print(f"Info: Cannot send internal follow-up. Lead '{lead_id}' status is '{lead['status']}'.")
        return

    followup_subject = f"Following up: {lead['contact_name']} at {lead['company_name']}"
    followup_body = f"""
    Hi {lead['contact_name']},

    Just wanted to gently follow up on my previous email. I know you're busy, but I genuinely believe that our AI Outreach Pipeline could bring significant value to {lead['company_name']}.

    If now isn't the best time, perhaps you could suggest a better moment to connect?

    Looking forward to hearing from you.

    Best regards,

    Junaid (AI Outreach Pipeline)
    """
    try:
        print(f"--- Simulating Automated Follow-up for {lead['contact_name']} ---")
        print(f"To: {lead['email']}")
        print(f"Subject: {followup_subject}")
        print(f"Body:\n{followup_body}")
        print(f"--------------------------------------")

        current_time_iso = datetime.datetime.now().isoformat()
        cursor.execute('UPDATE leads SET status = ?, sent_date = ? WHERE id = ?',
                       ('followup_sent', current_time_iso, lead_id))
        conn.commit()
        print(f"Automated follow-up successful for lead {lead_id}. Status updated to 'followup_sent'.")

    except sqlite3.Error as e:
        print(f"Error: Database error updating status for lead {lead_id} during automated follow-up: {e}")
    except Exception as e:
        print(f"Error: An unexpected error occurred during automated follow-up for lead {lead_id}: {e}")

if __name__ == '__main__':
    init_db() # Ensure DB is set up before running the app

    # NEW: Schedule the automated follow-up job
    # We'll run it every 10 seconds for testing purposes.
    # In a real app, this might be once every few hours or daily.
    scheduler.add_job(automated_followup_check, 'interval', seconds=10)
    scheduler.start()
    print("Scheduler started. Automated follow-up checks will run every 10 seconds.")

    try:
        app.run(debug=True, port=5000)
    except (KeyboardInterrupt, SystemExit):
        # Shut down the scheduler when the app stops
        scheduler.shutdown()
        print("Scheduler shut down gracefully.")