payload_records = []
prim_df, rel_df = AE_V.copy(), FA_V.copy()
if len(prim_df) > 0:
    args_dict = {
        'prim_form_ls': ['AE_F'],
        'prim_visit_ls': [],
        'seconon_form_ls': ['FA_PE_ISA'],
        'seconon_visit_ls': [],
        'prim_vable1': '_V_INJECTIONSITE',
        'prim_vable2': 'AEREFID',
        'prim_value': 'Y',
        'equl_op': '==',
        'not_equl_op': '!=',
    }
    prim_form_ls, prim_visit_ls = args_dict['prim_form_ls'], args_dict['prim_visit_ls']
    seconon_form_ls, seconon_visit_ls = args_dict['seconon_form_ls'], args_dict['seconon_visit_ls']
    prim_vable1 = args_dict['prim_vable1']
    prim_vable2 = args_dict['prim_vable2']
    not_equl_op = args_dict['not_equl_op']
    prim_df = udf_glbl_filter_by_formid_visitid(prim_df, prim_form_ls, prim_visit_ls)

    fa_aerefid_val = []
    if len(rel_df) > 0:
        rel_df = udf_glbl_filter_by_formid_visitid(rel_df, seconon_form_ls, seconon_visit_ls)
    if len(rel_df) > 0:
        rel_refids_r_c = [col for col in rel_df.columns if col.startswith('FALNKID')]
        if len(rel_refids_r_c) > 0:
            for x in rel_refids_r_c:
                fa_aerefid_val.extend(rel_df[x].str.upper().unique().tolist())

    if prim_df.shape[0] > 0:
        prim_df = prim_df[prim_df[prim_vable1].isin(['Y', 'y', 'YES', 'yes', 'Yes', 1, '1'])]
        if prim_df.shape[0] > 0:
            prim_flag, prim_df = udf_glbl_null_check(prim_df, prim_vable2, not_equl_op)
            if prim_flag and len(prim_df) > 0:
                prim_df = prim_df.sort_values(by='SPID', ascending=True).drop_duplicates(
                    subset=prim_vable2, keep='first').sort_index().reset_index(drop=True)

                for prim_ind in range(prim_df.shape[0]):
                    prim_rec = prim_df.iloc[[prim_ind]]
                    aerefid = prim_rec[prim_vable2].values[0].upper()
                    if aerefid not in fa_aerefid_val:
                        payload = {
                            "query_text": query_text ,
                            "form_index": str(prim_rec['form_index'].values[0]),
                            "modif_dts": str(pd.to_datetime(prim_rec['modif_dts'].values[0])),
                            "stg_ck_event_id": int(prim_rec['ck_event_id']),
                            "relational_ck_event_ids": [],
                            "confid_score": 1,
                        }
                        payload_records.append(payload)
