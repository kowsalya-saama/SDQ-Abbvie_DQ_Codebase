payload_records = []
prim_df = LB_LOCAL_V
if prim_df.shape[0] > 0:
    args_dict = {
        'p_form_ls': ['LB_UM2_F'],
        'p_visit_ls': [],
        'logic_text': "(LBCAT.str.upper() == 'URINE MICROSCOPY') and (LBNAM not in @null_values) and (LBDTC.isna() or LBDTC in @null_values)",
        'null_values': ['null', 'Null', 'NULL', 'NaN', '', ' ', 'None', 'NaT', 'np.nan', 'nan', None,
                        np.nan],
    }
    p_form_ls = args_dict['p_form_ls']
    p_visit_ls = args_dict['p_visit_ls']
    null_values = args_dict['null_values']
    logic_query = args_dict['logic_text']
    prim_df = udf_glbl_filter_by_form_visit(prim_df, p_form_ls, p_visit_ls)
    if len(prim_df) > 0:
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
