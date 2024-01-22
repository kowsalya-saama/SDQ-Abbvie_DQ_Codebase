prim_df = TRIGGER.copy()
rel_df = MB_V.copy()
payload_records = []
if prim_df.shape[0] > 0 and rel_df.shape[0] > 0:
    args_dict = {
        'p_form_ls': ['SAE_TRIG_F'],
        'p_visit_ls': [],
        's_form_ls': ['MB_F'],
        's_visit_ls': [],
        'p_var1': '_V_SM',
        's_var': 'MBDTC',
        'p_val': ['Y', 'y', 'Yes', 'yes', 'YES']
    }

    p_form_ls = args_dict['p_form_ls']
    p_visit_ls = args_dict['p_visit_ls']
    s_form_ls = args_dict['s_form_ls']
    s_visit_ls = args_dict['s_visit_ls']
    p_vpr, s_prtrt, val = args_dict['p_var1'], args_dict['s_var'], args_dict['p_val']

    prim_df = udf_glbl_filter_by_form_visit(prim_df, p_form_ls, p_visit_ls)
    rel_df = udf_glbl_filter_by_form_visit(rel_df,s_form_ls,s_visit_ls)

    if len(prim_df) > 0:
        prim_df = prim_df[prim_df[p_vpr].isin(val)]
    if len(rel_df)>0:
        r_flag, rel_df = udf_glbl_null_check(rel_df,s_prtrt,'==')

    if len(prim_df) > 0 and len(rel_df)>0:
        for prim_ind in prim_df.index.tolist():
            prim_rec = prim_df.loc[[prim_ind]]
            prim_visit_ix = prim_rec['visit_ix'].values[0]
            temp_rel_df = rel_df[rel_df['visit_ix'] == prim_visit_ix]
            if len(temp_rel_df)>0:
                ck_event = []
                new_rel_df = temp_rel_df.sort_values(['SPID'], ascending=True)
                ck_event.append(int(new_rel_df['ck_event_id'].values[0]))
                payload = {
                    "query_text": query_text,  # Update the query text here
                    "form_index": str(prim_rec['form_index'].values[0]),
                    "modif_dts": str(pd.to_datetime(prim_rec['modif_dts'].values[0])),
                    "stg_ck_event_id": int(prim_rec['ck_event_id']),
                    "relational_ck_event_ids": ck_event,
                    "confid_score": 1,
                }
                payload_records.append(payload)
