import pandas as pd


# Change the columns type while import the data to make sure the leading 0 are correctly included
NRI = pd.read_csv('NRI_Table_Counties.csv', dtype={'STCOFIPS': str})
NRI['STCOFIPS'].unique()

NRI_Sub = NRI.filter(regex='(_AFREQ|_RISKR)$')
NRI_Sub = NRI_Sub.join(NRI[['STCOFIPS']]) ## Also include the 5-digit state/county FIPS Code 

# Make sure the unique value is correct
NRI_Sub['STCOFIPS'].nunique()

# __3. Create a table / dataframe that, for each hazard type, 
# shows the number of missing values in the '\_AFREQ' and '\_RISKR' columns.
NRI_Missing = NRI_Sub.melt(var_name='Column', value_name='Value')
NRI_Missing = NRI_Missing.groupby('Column')['Value'].apply(lambda x: x.isnull().sum()).reset_index()
NRI_Missing['Type'] = NRI_Missing['Column'].str[-5:]
NRI_Missing['Hazard_Type'] = NRI_Missing['Column'].str[:4] 
NRI_Missing = NRI_Missing[NRI_Missing['Column'] != 'STCOFIPS']
NRI_Missing = NRI_Missing.drop(columns={'Column'})

NRI_Missing = pd.pivot_table(NRI_Missing, values=['Value'],
                             index=['Hazard_Type'],
                             columns=['Type'],
                             aggfunc="sum",
                             fill_value=0).reset_index()

NRI_Missing.columns = NRI_Missing.columns.droplevel(0)
new_column_names = ['Hazard_Type', 'Missing_AFREQ', 'Missing_RISKR']
NRI_Missing.columns = new_column_names
print(NRI_Missing)

cross_tab = pd.crosstab(NRI_Sub['AVLN_AFREQ'], NRI_Sub['AVLN_RISKR'], dropna=False)
NRI_Sub['AVLN_RISKR'].unique()
cross_tab

AFREQ_col = NRI_Sub.filter(regex='_AFREQ$')
NRI_Sub[AFREQ_col.columns] = AFREQ_col.fillna(0)

SVI = pd.read_csv('SVI_2022_US_county.csv', dtype={'FIPS':str})

columns_to_use = [
    'ST', 'STATE', 'ST_ABBR', 'STCNTY', 'COUNTY', 'FIPS', 'LOCATION', 'AREA_SQMI', 'E_TOTPOP', 'EP_POV150', 
    'EP_UNEMP', 'EP_HBURD', 'EP_NOHSDP', 'EP_UNINSUR', 'EP_AGE65', 'EP_AGE17', 'EP_DISABL', 'EP_SNGPNT', 
    'EP_LIMENG', 'EP_MINRTY', 'EP_MUNIT', 'EP_MOBILE', 'EP_CROWD', 'EP_NOVEH', 'EP_GROUPQ', 'EP_NOINT', 
    'EP_AFAM', 'EP_HISP', 'EP_ASIAN', 'EP_AIAN', 'EP_NHPI', 'EP_TWOMORE', 'EP_OTHERRACE'
]

SVI_Sub = SVI[columns_to_use]
print(SVI_Sub.columns)

# Function to calculate the number of missing values in each column
def missing_val(table):
    missing = table.isnull().sum()
    return pd.DataFrame(missing, columns=['Missing_Val'])

# Applying the function to the loaded DataFrame
SVI_Missing = missing_val(SVI_Sub)

# Display the resulting DataFrame (Apparently there are no missing values)
SVI_Missing

# Check the original subset table to see if the function was written correctly
SVI_Sub.columns[SVI_Sub.isnull().any()]

# First get the FIPS code from both dataset
NRI_FIPS = NRI['STCOFIPS']
SVI_FIPS = SVI['FIPS']

# FIPS codes that are in NRI but not in SVI
# FIPS codes that are in SVI but not in NRI
FIPS_NRI_NotSVI = NRI_FIPS[~NRI_FIPS.isin(SVI_FIPS)]
FIPS_SVI_NotNRI = SVI_FIPS[~SVI_FIPS.isin(NRI_FIPS)]

# See what are the missing gepgraphy in the SVI
FIPS_NRI_NotSVI_check = FIPS_NRI_NotSVI.to_list()
NRI[NRI['STCOFIPS'].isin(FIPS_NRI_NotSVI_check)]['STATE'].unique()

FIPS_SVI_NotNRI_check = FIPS_SVI_NotNRI.to_list()
SVI[SVI['FIPS'].isin(FIPS_SVI_NotNRI_check)]

# __2. Merge the NRI and SVI data on the FIPS code. Use an outer join to keep all counties in the final dataset.
Merge_SVI_NRI = pd.merge(NRI, SVI, left_on='STCOFIPS', right_on='FIPS', how= 'outer')
Merge_SVI_NRI.to_csv('01_clean_data.csv')
