import pandas as pd
import numpy as np
import glob
import os

# Helper function: Automatically find the latest file matching a pattern
def get_latest_file(pattern):
    files = glob.glob(pattern)
    if not files:
        return None
    # Sort by modification time and return the latest file
    return max(files, key=os.path.getmtime)

def process_asset_inventory():
    print("Starting smart scan and loading data sources...")
    
    # 1. Load the Master Inventory (Assuming this filename is constant)
    inv_file = 'Asset_Inventory.csv'
    if not os.path.exists(inv_file):
        print(f"FATAL ERROR: Cannot find master inventory '{inv_file}'. Script terminated.")
        return
    df_inv = pd.read_csv(inv_file, engine='python', on_bad_lines='warn')

    # Dynamically fetch the latest source files
    vmware_file = get_latest_file('*VM Inventory.csv')
    proxmox_file = get_latest_file('*Proxmox*.csv')
    ths_file = get_latest_file('*latest_agents*.csv')

    # Prepare empty dataframes in case a file is missing
    df_vmware_final = pd.DataFrame()
    df_proxmox_final = pd.DataFrame()

    # ==========================================
    # STEP 1: VMWARE PROCESSING LOGIC
    # ==========================================
    if vmware_file:
        print(f"Found VMware file: {vmware_file}. Processing...")
        df_vmware = pd.read_csv(vmware_file, engine='python', on_bad_lines='warn')
        
        if 'Power state' in df_vmware.columns:
            df_vmware = df_vmware[df_vmware['Power state'].str.strip().str.lower() == 'powered on']
 
        if 'Replica' in df_vmware.columns:
            df_vmware = df_vmware[df_vmware['Replica'].str.strip().str.lower() == 'False']
    
        if 'Template' in df_vmware.columns:
            df_vmware = df_vmware[df_vmware['Template'].str.strip().str.lower() == 'False']
            
        df_vmware = df_vmware[~df_vmware['Name'].str.contains('template|replica|migrated', case=False, na=False)]
        
        # Filter Client Assets: Exclude rows where 'Microsoft Windows' is directly followed by a number
        if 'OS System' in df_vmware.columns:
            df_vmware = df_vmware[~df_vmware['OS System'].astype(str).str.contains(r'Microsoft Windows \d+', case=False, regex=True, na=False)]

        vm_rename_map = {
            'vCenter': 'Cluster',
            'OS System': 'OS',
            'Functional Group': 'Functional_Group',
            'Business Application': 'Application',
            'OS Technical Maintainer': 'OS_Technical_Maintainer'
        }
        vm_cols_to_keep = ['Name', 'Environment', 'CICollection', 'Organization']
        available_vm_cols = [c for c in vm_rename_map.keys() if c in df_vmware.columns] + \
                            [c for c in vm_cols_to_keep if c in df_vmware.columns]
        
        df_vmware_final = df_vmware[available_vm_cols].rename(columns=vm_rename_map).copy()
        
        # Build VMware 'Location' column based on Cluster prefix
        if 'Cluster' in df_vmware_final.columns:
            cluster_upper = df_vmware_final['Cluster'].astype(str).str.upper()
            
            # Condition for Valencia: Starts with 'VLC', exactly matches 'DFS-VCS-51', or contains 'EDCV'
            cond_valencia = (
                cluster_upper.str.startswith('VLC') | 
                (cluster_upper == 'DFS-VCS-51') | 
                cluster_upper.str.contains('EDCV', na=False)
            )
            
            # Condition for Brindisi: Starts with 'BDS', exactly matches 'DFS-VCS-01', 
            # or contains 'EDC' (but explicitly MUST NOT contain 'EDCV' to prevent overlap)
            cond_brindisi = (
                cluster_upper.str.startswith('BDS') | 
                (cluster_upper == 'DFS-VCS-01') | 
                (cluster_upper.str.contains('EDC', na=False) & ~cluster_upper.str.contains('EDCV', na=False))
            )
            
            df_vmware_final['Location'] = np.select([cond_brindisi, cond_valencia], ['Brindisi', 'Valencia'], default='Unknown')
        
        df_vmware_final['Technology_Source'] = 'VMware'
        df_vmware_final = df_vmware_final.replace([np.nan, '-', '', ' ', '- '], 'Unknown')
    else:
        print("No VMware source file detected. Skipping VMware processing.")

    # ==========================================
    # STEP 2: PROXMOX PROCESSING LOGIC
    # ==========================================
    if proxmox_file:
        print(f"Found Proxmox Excel file: {proxmox_file}. Processing specific sheet...")
        # Read the specific sheet and skip the first 3 rows
        df_proxmox = pd.read_excel(proxmox_file, sheet_name='VMs all discoverd', skiprows=3, engine='openpyxl')
        
        if 'powerstate' in df_proxmox.columns:
            df_proxmox = df_proxmox[df_proxmox['powerstate'].str.strip().str.lower() == 'poweredon']
            
        df_proxmox = df_proxmox[~df_proxmox['name'].str.contains('template|replica|migrated', case=False, na=False)]

     # Filter Client Assets for Proxmox: Exclude rows where 'Microsoft Windows' is directly followed by a number
        if 'DiscoveredOsName' in df_proxmox.columns:
            df_proxmox = df_proxmox[~df_proxmox['DiscoveredOsName'].astype(str).str.contains(r'Microsoft Windows \d+', case=False, regex=True, na=False)]
            
        px_rename_map = {
            'name': 'Name',
            'cluster_node': 'Cluster',
            'DiscoveredApplicationMaintainer': 'Functional_Maintainer',
            'DiscoveredApplication': 'Application',
            'DiscoveredCICollection': 'CICollection',
            'DiscoveredEnvironment': 'Environment',
            'DiscoveredOSTechnicalMaintainer': 'OS_Technical_Maintainer',
            'DiscoveredOrganization': 'Organization',
            'ipaddress': 'IP_Address',
            'DiscoveredOsName': 'OS'
        }
        px_cols_to_keep = ['Location']
        available_px_cols = [c for c in px_rename_map.keys() if c in df_proxmox.columns] + \
                            [c for c in px_cols_to_keep if c in df_proxmox.columns]
        
        df_proxmox_final = df_proxmox[available_px_cols].rename(columns=px_rename_map).copy()

        # Normalize Proxmox 'Location'
        if 'Location' in df_proxmox_final.columns:
            loc_upper = df_proxmox_final['Location'].astype(str).str.upper()
            df_proxmox_final['Location'] = np.select(
                [loc_upper == 'BDS', loc_upper == 'VLC'],
                ['Brindisi', 'Valencia'],
                default=df_proxmox_final['Location']
            )
            
        df_proxmox_final['Technology_Source'] = 'Proxmox'
        df_proxmox_final = df_proxmox_final.replace([np.nan, '-', '', ' ', '- '], 'Unknown')
    else:
        print("No Proxmox source file detected. Skipping Proxmox processing.")

    # ==========================================
    # STEP 3: MASTER INVENTORY UPDATE
    # ==========================================
    print("Comparing and updating Master Asset Inventory...")
    source_assets = {}
    
    # Safe to iterate even if dataframes are empty
    for _, row in df_vmware_final.iterrows():
        source_assets[str(row['Name']).strip().lower()] = row.to_dict()
    for _, row in df_proxmox_final.iterrows():
        source_assets[str(row['Name']).strip().lower()] = row.to_dict()

    # Create lowercase match key
    df_inv['Name_lower'] = df_inv['VM_Name'].astype(str).str.strip().str.lower()
    inv_names = set(df_inv['Name_lower'])
    
    def get_updated_status(name_lower):
        if name_lower in source_assets:
            return 'Existing'
        return 'Removed'
            
    df_inv['Status'] = df_inv['Name_lower'].apply(get_updated_status)
    
    new_assets = []
    for name_lower, row_data in source_assets.items():
        if name_lower not in inv_names:
            new_row = {
                'VM_Name': row_data.get('Name', 'Unknown'),
                'Application': row_data.get('Application', 'Unknown'),
                'CICollection': row_data.get('CICollection', 'Unknown'),
                'Cluster': row_data.get('Cluster', 'Unknown'),
                'Functional_Group': row_data.get('Functional_Group', row_data.get('Functional_Maintainer', 'Unknown')),
                'Environment': row_data.get('Environment', 'Unknown'),
                'IP_Address': row_data.get('IP_Address', 'Unknown'),
                'Location': row_data.get('Location', 'Unknown'),
                'Organization': row_data.get('Organization', 'Unknown'),
                'OS': row_data.get('OS', 'Unknown'),
                'OS_Technical_Maintainer': row_data.get('OS_Technical_Maintainer', 'Unknown'),
                'Status': 'Newly Added',
                'Technology': row_data.get('Technology_Source', 'Unknown'),
                'THS deployment': 'Unknown',
                'THS_System covered by GRR': 'Unknown',
                'THS_System covered by Sysmon': 'Unknown',
                'THS_System logs shipped': 'Unknown',
            }
            new_assets.append(new_row)
            
    if new_assets:
        df_new = pd.DataFrame(new_assets)
        df_inv = pd.concat([df_inv, df_new], ignore_index=True)


    # ==========================================
    # STEP 4: MAP THS AGENT COLUMNS
    # ==========================================
    if ths_file:
        print(f"Found THS Agent file: {ths_file}. Mapping data...")
        df_ths = pd.read_csv(ths_file, skiprows=3, engine='python', on_bad_lines='warn')
        df_ths['Match_Name'] = df_ths['Hostname'].astype(str).str.strip().str.lower()
        df_ths = df_ths.drop_duplicates(subset=['Match_Name'])
        ths_dict = df_ths.set_index('Match_Name').to_dict('index')

        # Ensure 'Name_lower' covers any newly added rows
        df_inv['Name_lower'] = df_inv['VM_Name'].astype(str).str.strip().str.lower()

        def apply_ths_data(row):
            match_name = row['Name_lower']
            if match_name in ths_dict:
                ths_data = ths_dict[match_name]
                row['THS deployment'] = ths_data.get('THS deployment', row['THS deployment'])
                row['THS_System covered by GRR'] = ths_data.get('System covered by GRR', row['THS_System covered by GRR'])
                row['THS_System logs shipped'] = ths_data.get('System logs shipped', row['THS_System logs shipped'])
                row['THS_System covered by Sysmon'] = ths_data.get('System covered by Sysmon', row['THS_System covered by Sysmon'])
            return row

        df_inv = df_inv.apply(apply_ths_data, axis=1)
    else:
        print("No THS Agent file detected. Skipping THS mapping.")

# ==========================================
    # STEP 5: CLEANUP, SAVE & GENERATE LOG
    # ==========================================
    df_inv = df_inv.drop(columns=['Name_lower'])
    df_inv = df_inv.fillna('Unknown')
    df_inv = df_inv.replace([np.nan, '-', '', ' ', '- '], 'Unknown')
    
    out_file = 'Asset_Inventory_Updated.csv'
    df_inv.to_csv(out_file, index=False)
    
    # --- NEW LOGIC: Generate Automatic Log ---
    status_counts = df_inv['Status'].value_counts()
    existing_count = status_counts.get('Existing', 0)
    removed_count = status_counts.get('Removed', 0)
    newly_added_count = status_counts.get('Newly Added', 0)
    
    # Create beautifully formatted Markdown content
    log_content = f"""## 📊 Asset Inventory Update Summary
    
**Status Breakdown:**
* 🟢 **Existing (Matched):** {existing_count}
* 🔴 **Removed (Missing):** {removed_count}
* 🔵 **Newly Added:** {newly_added_count}

**Total assets in updated inventory:** {len(df_inv)}
"""
    
    # 1. Save to a persistent Markdown file in the repository
    with open('Update_Log.md', 'w', encoding='utf-8') as f:
        f.write(log_content)
        
    # 2. Push to GitHub Actions Step Summary UI (if running inside GitHub)
    if "GITHUB_STEP_SUMMARY" in os.environ:
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding='utf-8') as f:
            f.write(log_content)
    
    print("\n===============================")
    print("   UPDATE PROCESS COMPLETED!   ")
    print("===============================\n")
    print(log_content)
    print(f"Data successfully saved as: '{out_file}'")
    print("Log successfully saved as: 'Update_Log.md'")

if __name__ == "__main__":
    process_asset_inventory()
