payload_records = []
prim_df = PC
if prim_df.shape[0] > 0:
    args_dict = {
        'logic_text': "(PCSTAT not in @null_values and PCSTAT.str.upper() == 'NOT DONE') and (PCREASND not in @null_values)",
        'null_values': ['null', 'Null', 'NULL', 'NaN', '', ' ', 'None', 'NaT', 'np.nan', 'nan', None, np.nan],
        'refid_flag': False,
        'refid_col': []
    }

    null_values = args_dict['null_values']
    logic_query = args_dict['logic_text']

    prim_df = prim_df.query(logic_query)
    if len(prim_df) > 0:
        for prim_ind in prim_df.index.tolist():
            prim_rec = prim_df.loc[[prim_ind]]
            payload = {
                "query_text": query_text,
                "form_index": str(prim_rec['form_index'].values[0]),
                "modif_dts": str(pd.to_datetime(prim_rec['modif_dts'].values[0])),
                "stg_ck_event_id": int(prim_rec['ck_event_id']),
                "confid_score": 1,
            }
            payload_records.append(payload)
