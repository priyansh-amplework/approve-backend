"""
google_sheets_updater.py - Update Google Sheets status after posting
Writes back posting results to the spreadsheet
"""

from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
from datetime import datetime

# Configuration
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Column mapping (adjust if your sheet structure is different)
# A=username, B=name, C=badge_type, D=platforms, E=posted, F=post_date, G=post_urls, H=tracking_id
COLUMN_POSTED = "E"
COLUMN_POST_DATE = "F"
COLUMN_POST_URLS = "G"
COLUMN_TRACKING_ID = "H"


def get_sheets_service():
    """Create and return authenticated Google Sheets service"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE, 
            scopes=SCOPES
        )
        service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)
        return service
    except FileNotFoundError:
        raise Exception(f"Service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}")
    except Exception as e:
        raise Exception(f"Failed to authenticate with Google Sheets: {str(e)}")


def update_sheet_status(row_number, posted=False, post_urls='', tracking_id='', tracking_ids_dict=None, error=''):
    """
    Update the status columns in Google Sheet after posting
    
    Args:
        row_number: Row number to update (1-indexed, includes header)
        posted: Boolean - was the post successful?
        post_urls: URLs of the published posts (all platforms combined)
        tracking_id: Primary tracking ID (for backward compatibility)
        tracking_ids_dict: Dict mapping platform to tracking_id (e.g., {'facebook': 'aB3xK9', 'linkedin': 'xY4pQ2'})
        error: Error message if posting failed
    
    Returns:
        Boolean - True if update successful
    """
    try:
        service = get_sheets_service()
        sheet = service.spreadsheets()
        
        # Prepare the values to write
        if posted:
            # Format tracking IDs nicely for Google Sheet
            if tracking_ids_dict and len(tracking_ids_dict) > 1:
                # Multiple platforms: "FB:aB3xK9 | LI:xY4pQ2 | TW:mZ8nQ1"
                tracking_ids_formatted = " | ".join([
                    f"{platform[:2].upper()}:{tid}" 
                    for platform, tid in tracking_ids_dict.items()
                ])
            elif tracking_ids_dict and len(tracking_ids_dict) == 1:
                # Single platform: just the ID
                tracking_ids_formatted = list(tracking_ids_dict.values())[0]
            else:
                # Fallback to single tracking_id parameter
                tracking_ids_formatted = tracking_id
            
            values = [[
                "✅",  # Column E - posted status (checkmark)
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Column F - post_date
                post_urls,  # Column G - post_urls (all platforms combined)
                tracking_ids_formatted  # Column H - tracking_ids (all platforms)
            ]]
            
            print(f"   📝 Marking row {row_number} as POSTED (✅)")
            if tracking_ids_dict:
                print(f"   🆔 Tracking IDs: {tracking_ids_formatted}")
        else:
            # For failed posts, write error message
            error_display = f"❌ {error[:47]}" if error else "❌ FAILED"
            values = [[
                error_display,  # Column E - error message
                "",  # Column F - empty
                "",  # Column G - empty
                ""   # Column H - empty
            ]]
            
            print(f"   📝 Marking row {row_number} as FAILED: {error[:50]}")
        
        # Define the range to update (E to H for the specific row)
        range_name = f"Sheet1!{COLUMN_POSTED}{row_number}:{COLUMN_TRACKING_ID}{row_number}"
        
        body = {
            'values': values
        }
        
        # Update the sheet
        result = sheet.values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=range_name,
            valueInputOption='USER_ENTERED',  # This interprets dates, numbers, etc.
            body=body
        ).execute()
        
        updated_cells = result.get('updatedCells', 0)
        print(f"   ✅ Updated {updated_cells} cell(s) in Google Sheet")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to update Google Sheet row {row_number}: {e}")
        return False


def batch_update_sheet_status(updates):
    """
    Batch update multiple rows at once for efficiency
    
    Args:
        updates: List of dicts with keys: row_number, posted, post_urls, tracking_id, error
    
    Returns:
        int - Number of successful updates
    """
    if not updates:
        return 0
    
    try:
        service = get_sheets_service()
        sheet = service.spreadsheets()
        
        # Build batch update data
        data = []
        
        for update in updates:
            row_number = update.get('row_number')
            posted = update.get('posted', False)
            post_urls = update.get('post_urls', '')
            tracking_id = update.get('tracking_id', '')
            tracking_ids_dict = update.get('tracking_ids_dict', None)
            error = update.get('error', '')
            
            if posted:
                # Format tracking IDs
                if tracking_ids_dict and len(tracking_ids_dict) > 1:
                    tracking_ids_formatted = " | ".join([
                        f"{platform[:2].upper()}:{tid}" 
                        for platform, tid in tracking_ids_dict.items()
                    ])
                elif tracking_ids_dict and len(tracking_ids_dict) == 1:
                    tracking_ids_formatted = list(tracking_ids_dict.values())[0]
                else:
                    tracking_ids_formatted = tracking_id
                
                values = [[
                    "✅",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    post_urls,
                    tracking_ids_formatted
                ]]
            else:
                error_display = f"❌ {error[:47]}" if error else "❌ FAILED"
                values = [[error_display, "", "", ""]]
            
            range_name = f"Sheet1!{COLUMN_POSTED}{row_number}:{COLUMN_TRACKING_ID}{row_number}"
            
            data.append({
                'range': range_name,
                'values': values
            })
        
        # Execute batch update
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': data
        }
        
        result = sheet.values().batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body=body
        ).execute()
        
        total_updated = result.get('totalUpdatedCells', 0)
        print(f"   ✅ Batch updated {total_updated} cell(s) in Google Sheet")
        
        return len(data)
        
    except Exception as e:
        print(f"   ❌ Batch update failed: {e}")
        return 0


def read_posted_status(row_number):
    """
    Read the current posted status for a specific row
    
    Args:
        row_number: Row number to check
    
    Returns:
        dict with keys: posted (bool), post_date, post_urls, tracking_id
    """
    try:
        service = get_sheets_service()
        sheet = service.spreadsheets()
        
        range_name = f"Sheet1!{COLUMN_POSTED}{row_number}:{COLUMN_TRACKING_ID}{row_number}"
        
        result = sheet.values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [[]])
        
        if not values or not values[0]:
            return {
                'posted': False,
                'post_date': None,
                'post_urls': None,
                'tracking_id': None
            }
        
        row_data = values[0]
        
        # Pad with empty strings if columns are missing
        while len(row_data) < 4:
            row_data.append('')
        
        posted_status = row_data[0].strip()
        
        return {
            'posted': posted_status in ['✅', 'YES', 'TRUE', 'POSTED'],
            'post_date': row_data[1],
            'post_urls': row_data[2],
            'tracking_id': row_data[3]
        }
        
    except Exception as e:
        print(f"   ⚠️ Could not read status for row {row_number}: {e}")
        return {'posted': False, 'post_date': None, 'post_urls': None, 'tracking_id': None}


def get_all_posted_badges():
    """
    Get all badges that have been posted (for duplicate detection)
    
    Returns:
        List of dicts with posted badge info
    """
    try:
        service = get_sheets_service()
        sheet = service.spreadsheets()
        
        # Read all data including status columns
        range_name = "Sheet1!A:H"
        
        result = sheet.values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if len(values) < 2:
            return []
        
        # Skip header
        posted_badges = []
        
        for i, row in enumerate(values[1:], start=2):
            if len(row) < 5:
                continue
            
            posted_status = row[4] if len(row) > 4 else ''
            
            if posted_status in ['✅', 'YES', 'TRUE', 'POSTED']:
                badge_info = {
                    'row_number': i,
                    'username': row[0] if len(row) > 0 else '',
                    'name': row[1] if len(row) > 1 else '',
                    'badge_type': row[2] if len(row) > 2 else '',
                    'platforms': row[3] if len(row) > 3 else '',
                    'post_date': row[5] if len(row) > 5 else '',
                    'post_urls': row[6] if len(row) > 6 else '',
                    'tracking_id': row[7] if len(row) > 7 else ''
                }
                posted_badges.append(badge_info)
        
        return posted_badges
        
    except Exception as e:
        print(f"   ⚠️ Could not retrieve posted badges: {e}")
        return []


if __name__ == "__main__":
    # Test the updater
    print("Testing Google Sheets Updater...")
    print("="*70)
    
    # Test reading status
    print("\n1. Testing read_posted_status for row 2:")
    status = read_posted_status(2)
    print(f"   Result: {status}")
    
    # Test getting all posted badges
    print("\n2. Testing get_all_posted_badges:")
    posted = get_all_posted_badges()
    print(f"   Found {len(posted)} posted badge(s)")
    for badge in posted[:3]:  # Show first 3
        print(f"   • Row {badge['row_number']}: {badge['name']} - {badge['post_date']}")
    
    print("\n" + "="*70)
    print("✅ Test complete!")