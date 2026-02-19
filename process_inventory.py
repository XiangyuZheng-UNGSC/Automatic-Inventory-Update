import pandas as pd
import numpy as np

def run_full_inventory_pipeline():
    print("Loading datasets...")
    
    # 1. Load Files (Added skiprows=3 for the THS file)
    df_inv = pd.read_csv('Inventory.csv', engine='python', on_bad_lines='warn')
    df_vmware = pd.read_csv('2-2-26 VM Inventory - CuAttributes vSphere World.csv', engine='python', on_bad_lines='warn')
    df_proxmox = pd.read_csv('VMs all discovered in Proxmox.xlsx - VMs all discovered.csv', engine='python', on_bad_lines='warn')
    df_ths = pd.read_csv('latest_agents-20260114 UNGSC.xlsx - Sheet2.csv', skiprows=3, engine='python', on_bad_lines='warn')

    # ==========================================
    # STEP 1: VMWARE CLEANING & LOCATION
    # ==========================================
    print("Processing VMware data...")
    if 'Power state' in df_vmware.columns:
        df_vmware = df_vmware[df_vmware['Power state'].str.strip().str.lower() == 'powered on']
        
    if 'SRM Placeholder' in df_vmware.columns:
        df_vmware = df_vmware[~df_vmware['SRM Placeholder'].astype(str).str.lower().isin(['true', '1'])]
        
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
    
    # Build VMware 'Location' column
    if 'Cluster' in df_vmware_final.columns:
        cluster_upper = df_vmware_final['Cluster'].astype(str).str.upper()
        cond_brindisi = cluster_upper.str.startswith('BDS') | (cluster_upper == 'DFS-VCS-01') | (cluster_upper == 'DEC')
        cond_valencia = cluster_upper.str.startswith('VLC') | (cluster_upper == 'DFS-VCS-51') | (cluster_upper == 'EDCV')
        
        df_vmware_final['Location'] = np.select([cond_brindisi, cond_valencia], ['Brindisi', 'Valencia'], default='Unknown')
    
    df_vmware_final['Technology_Source'] = 'VMware'
    df_vmware_final = df_vmware_final.replace([np.nan, '-', '', ' ', '- '], 'Unknown')


    # ==========================================
    # STEP 2: PROXMOX CLEANING & LOCATION
    # ==========================================
    print("Processing Proxmox data...")
    if 'powerstate' in df_proxmox.columns:
        df_proxmox = df_proxmox[df_proxmox['powerstate'].str.strip().str.lower() == 'poweredon']
        
    df_proxmox = df_proxmox[~df_proxmox['name'].str.contains('template|replica|migrated', case=False, na=False)]
    if 'tags' in df_proxmox.columns:
        df_proxmox = df_proxmox[~df_proxmox['tags'].str.contains('template|replica', case=False, na=False)]

    # Filter Client Assets
    os_cols_px = ['ostype', 'DiscoveredOsName', 'OsDescription']
    for col in os_cols_px:
        if col in df_proxmox.columns:
            df_proxmox = df_proxmox[~df_proxmox[col].astype(str).str.contains('Windows 10|Windows 11|win10|win11', case=False, na=False)]

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


    # ==========================================
    # STEP 3: MASTER INVENTORY UPDATE
    # ==========================================
    print("Updating Master Inventory...")
    source_assets = {}
    
    for _, row in df_vmware_final.iterrows():
        source_assets[str(row['Name']).strip().lower()] = row.to_dict()
    for _, row in df_proxmox_final.iterrows():
        source_assets[str(row['Name']).strip().lower()] = row.to_dict()

    df_inv['Name_lower'] = df_inv['Name'].astype(str).str.strip().str.lower()
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
                'Name': row_data.get('Name', 'Unknown'),
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
                'THS_System logs shipped': 'Unknown',
                'THS_System covered by Sysmon': 'Unknown'
            }
            new_assets.append(new_row)
            
    if new_assets:
        df_new = pd.DataFrame(new_assets)
        df_inv = pd.concat([df_inv, df_new], ignore_index=True)


    # ==========================================
    # PURGE CLIENTS FROM MASTER INVENTORY
    # Physically delete historical Win 10/11 assets
    # ==========================================
    if 'OS' in df_inv.columns:
        df_inv = df_inv[~df_inv['OS'].astype(str).str.contains(r'Windows 10|Windows 11', case=False, regex=True, na=False)]


    # ==========================================
    # STEP 4: MAP THS AGENT COLUMNS
    # ==========================================
    print("Mapping THS agent data...")
    df_ths['Match_Name'] = df_ths['Hostname'].astype(str).str.strip().str.lower()
    df_ths = df_ths.drop_duplicates(subset=['Match_Name'])
    ths_dict = df_ths.set_index('Match_Name').to_dict('index')

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

    # ==========================================
    # STEP 5: CLEANUP & SAVE
    # ==========================================
    df_inv = df_inv.drop(columns=['Name_lower'])
    df_inv = df_inv.fillna('Unknown')
    df_inv = df_inv.replace([np.nan, '-', '', ' ', '- '], 'Unknown')
    
    out_file = 'Final_Inventory_Complete.csv'
    df_inv.to_csv(out_file, index=False)
    
    print("\n--- Final Inventory Summary ---")
    print(df_inv['Status'].value_counts())
    print(f"\nSuccess! Fully processed inventory saved as '{out_file}'")

if __name__ == "__main__":
    run_full_inventory_pipeline()
