package org.apache.gdr.ext.util;

import org.apache.gdr.common.exception.GdrException;
import junit.framework.Assert;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Test;

public class TestTdFromHive {
    @Test
    public void testTextDDL() throws SemanticException, ParseException, GdrException {
        String command = "CREATE EXTERNAL TABLE dw_users_info_n_w(user_regn_id decimal(6,0), user_id decimal(18,0), user_slctd_id string, user_sts_code decimal(3,0), user_confirm_code string, last_modfd timestamp, userid_last_chg timestamp, user_flags decimal(16,0), email string, hash_email binary, user_cntry_id decimal(10,0), uvdetail decimal(16,0), uvrating decimal(5,0), feedback_score decimal(18,0), user_site_id decimal(3,0), cobrand_prtnr_id decimal(9,0), blng_curncy_id decimal(3,0), equifax_sts string, user_sex_flag decimal(18,0), top_slr_lvl_code decimal(3,0), flagsex2 decimal(18,0), motors_seller_level decimal(8,0), mbyte_userid string, flagsex3 decimal(18,0), flagsex4 int, flagsex5 int, flagsex6 int, geo_pstl_code string, bbp_min_alwd_fdbk_score int, ebx_elgbl_slr_yn_id tinyint, ebx_qlfd_slr_yn_id tinyint, slr_ebx_optn_status_id tinyint, ebx_cs_frc_out_yn_id tinyint, ebx_cs_frc_in_yn_id tinyint, ebx_regstrd_user_yn_id tinyint, sdc_sys_user_yn_id tinyint, regstrd_slng_chnl_id tinyint, flagsex7 int, guest_ind string, pp_auto_linking_disabled_ind string, flagsex8 int, flagsex9 int, flagsex10 int, flagsex11 int, acct_type_cd decimal(4,0), flagsex12 decimal(18,0), addr1 string, addr2 string, flags01 int, addr2_used_yn_id tinyint, banner_prtnr_id decimal(9,0), cc_onfile_yn string, city string, comp string, dayphone string, equifax_attempts decimal(4,0), equifax_last_modfd_date timestamp, faxphone string, gender_mfu string, good_crd_yn string, nightphone string, pref_categ_interest1_id decimal(18,0), pref_categ_interest2_id decimal(18,0), pref_categ_interest3_id decimal(18,0), pref_categ_interest4_id decimal(18,0), pstl_code string, state string, top_slr_initiate_date timestamp, user_cre_date timestamp, user_cre_prd_id string, user_cre_week_id string, user_name string, user_ip_addr string, req_email_count decimal(10,0), last_modified_user_info timestamp, payment_type decimal(9,0), date_confirm timestamp, date_of_birth timestamp, site_personal_id string, eop_verify_stat decimal(4,0), eop_last_verify timestamp, last_banner_prtnr_id decimal(9,0), aol_master_id decimal(18,0), tax_id_application_date timestamp, tax_id_confirm_date timestamp, tax_status decimal(3,0), motors_seller_initiated_date date, linked_paypal_acct string, paypal_link_state decimal(2,0), verification_method decimal(3,0), verification_type_code decimal(8,0), verification_date date, cellphone string, anonymous_email_yn_flag_id tinyint, reg_test_grp_id smallint, busn_type_id tinyint, user_dsgntn_id tinyint, user_dsgntn_dt date, user_dsgntn_tm timestamp, ebx_pref_last_modfd_dt date, ebx_pref_last_modfd_tm timestamp, pri_user_id decimal(18,0), top_byr_gmb_score int, smsphone string, slr_pp_dflt_email string, slr_reg_ip_addr string, user_first_name string, user_last_name string, reg_initd_site_id decimal(4,0), reg_cmpltd_site_id decimal(4,0), syi_block_begin_dt date, syi_block_dt date, slr_block_rsn_type decimal(4,0), cs_force_bsns_dt date, cs_bsns_exmptn_dt date, exclude_ship_to_loc_modify_ts timestamp, hash_addr1 binary, hash_addr2 binary, hash_dayphone binary, hash_faxphone binary, hash_nightphone binary, hash_user_name binary, hash_user_ip_addr binary, hash_cellphone binary, hash_smsphone binary, hash_slr_pp_dflt_email binary, hash_user_first_name binary, hash_user_last_name binary, payment_type_last_mdfd_date timestamp, user_prfr_lang_cd_txt string, biz_entity_type_txt string, trade_rgstrtn_id string, reg_mchn_group_id decimal(38,0), dayphone_cntry_cd decimal(4,0), address_count decimal(9,0), paybox_country_id string, paybox_number string, cbt_sbscr_dt date, hash_initl_real_email binary)"
                + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
                + "WITH SERDEPROPERTIES ("
                + "  'line.delim' = '\n'," + "  'field.delim' = '\177'" + ")"
                + "STORED AS"
                + "  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'"
                + "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
                + "LOCATION 'hdfs://ignite1-1-1794160.lvs02.dev.ebayc3.com:8020/data/dw_users_info_n_w'";
        //Log.info(hsp.getDML());
        String dmlStr = "CREATE MULTISET TABLE working.dw_users_info_n_w\n(\n  user_regn_id DECIMAL(6,0),\n  user_id DECIMAL(18,0),\n  user_slctd_id VARCHAR(100),\n  user_sts_code DECIMAL(3,0),\n  user_confirm_code VARCHAR(100),\n  last_modfd TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  userid_last_chg TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  user_flags DECIMAL(16,0),\n  email VARCHAR(100),\n  hash_email VARBYTE(32),\n  user_cntry_id DECIMAL(10,0),\n  uvdetail DECIMAL(16,0),\n  uvrating DECIMAL(5,0),\n  feedback_score DECIMAL(18,0),\n  user_site_id DECIMAL(3,0),\n  cobrand_prtnr_id DECIMAL(9,0),\n  blng_curncy_id DECIMAL(3,0),\n  equifax_sts VARCHAR(100),\n  user_sex_flag DECIMAL(18,0),\n  top_slr_lvl_code DECIMAL(3,0),\n  flagsex2 DECIMAL(18,0),\n  motors_seller_level DECIMAL(8,0),\n  mbyte_userid VARCHAR(100),\n  flagsex3 DECIMAL(18,0),\n  flagsex4 INTEGER,\n  flagsex5 INTEGER,\n  flagsex6 INTEGER,\n  geo_pstl_code VARCHAR(100),\n  bbp_min_alwd_fdbk_score INTEGER,\n  ebx_elgbl_slr_yn_id BYTEINT,\n  ebx_qlfd_slr_yn_id BYTEINT,\n  slr_ebx_optn_status_id BYTEINT,\n  ebx_cs_frc_out_yn_id BYTEINT,\n  ebx_cs_frc_in_yn_id BYTEINT,\n  ebx_regstrd_user_yn_id BYTEINT,\n  sdc_sys_user_yn_id BYTEINT,\n  regstrd_slng_chnl_id BYTEINT,\n  flagsex7 INTEGER,\n  guest_ind VARCHAR(100),\n  pp_auto_linking_disabled_ind VARCHAR(100),\n  flagsex8 INTEGER,\n  flagsex9 INTEGER,\n  flagsex10 INTEGER,\n  flagsex11 INTEGER,\n  acct_type_cd DECIMAL(4,0),\n  flagsex12 DECIMAL(18,0),\n  addr1 VARCHAR(100),\n  addr2 VARCHAR(100),\n  flags01 INTEGER,\n  addr2_used_yn_id BYTEINT,\n  banner_prtnr_id DECIMAL(9,0),\n  cc_onfile_yn VARCHAR(100),\n  city VARCHAR(100),\n  comp VARCHAR(100),\n  dayphone VARCHAR(100),\n  equifax_attempts DECIMAL(4,0),\n  equifax_last_modfd_date TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  faxphone VARCHAR(100),\n  gender_mfu VARCHAR(100),\n  good_crd_yn VARCHAR(100),\n  nightphone VARCHAR(100),\n  pref_categ_interest1_id DECIMAL(18,0),\n  pref_categ_interest2_id DECIMAL(18,0),\n  pref_categ_interest3_id DECIMAL(18,0),\n  pref_categ_interest4_id DECIMAL(18,0),\n  pstl_code VARCHAR(100),\n  state VARCHAR(100),\n  top_slr_initiate_date TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  user_cre_date TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  user_cre_prd_id VARCHAR(100),\n  user_cre_week_id VARCHAR(100),\n  user_name VARCHAR(100),\n  user_ip_addr VARCHAR(100),\n  req_email_count DECIMAL(10,0),\n  last_modified_user_info TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  payment_type DECIMAL(9,0),\n  date_confirm TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  date_of_birth TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  site_personal_id VARCHAR(100),\n  eop_verify_stat DECIMAL(4,0),\n  eop_last_verify TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  last_banner_prtnr_id DECIMAL(9,0),\n  aol_master_id DECIMAL(18,0),\n  tax_id_application_date TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  tax_id_confirm_date TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  tax_status DECIMAL(3,0),\n  motors_seller_initiated_date DATE FORMAT 'YYYY-MM-DD',\n  linked_paypal_acct VARCHAR(100),\n  paypal_link_state DECIMAL(2,0),\n  verification_method DECIMAL(3,0),\n  verification_type_code DECIMAL(8,0),\n  verification_date DATE FORMAT 'YYYY-MM-DD',\n  cellphone VARCHAR(100),\n  anonymous_email_yn_flag_id BYTEINT,\n  reg_test_grp_id SMALLINT,\n  busn_type_id BYTEINT,\n  user_dsgntn_id BYTEINT,\n  user_dsgntn_dt DATE FORMAT 'YYYY-MM-DD',\n  user_dsgntn_tm TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  ebx_pref_last_modfd_dt DATE FORMAT 'YYYY-MM-DD',\n  ebx_pref_last_modfd_tm TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  pri_user_id DECIMAL(18,0),\n  top_byr_gmb_score INTEGER,\n  smsphone VARCHAR(100),\n  slr_pp_dflt_email VARCHAR(100),\n  slr_reg_ip_addr VARCHAR(100),\n  user_first_name VARCHAR(100),\n  user_last_name VARCHAR(100),\n  reg_initd_site_id DECIMAL(4,0),\n  reg_cmpltd_site_id DECIMAL(4,0),\n  syi_block_begin_dt DATE FORMAT 'YYYY-MM-DD',\n  syi_block_dt DATE FORMAT 'YYYY-MM-DD',\n  slr_block_rsn_type DECIMAL(4,0),\n  cs_force_bsns_dt DATE FORMAT 'YYYY-MM-DD',\n  cs_bsns_exmptn_dt DATE FORMAT 'YYYY-MM-DD',\n  exclude_ship_to_loc_modify_ts TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  hash_addr1 VARBYTE(32),\n  hash_addr2 VARBYTE(32),\n  hash_dayphone VARBYTE(32),\n  hash_faxphone VARBYTE(32),\n  hash_nightphone VARBYTE(32),\n  hash_user_name VARBYTE(32),\n  hash_user_ip_addr VARBYTE(32),\n  hash_cellphone VARBYTE(32),\n  hash_smsphone VARBYTE(32),\n  hash_slr_pp_dflt_email VARBYTE(32),\n  hash_user_first_name VARBYTE(32),\n  hash_user_last_name VARBYTE(32),\n  payment_type_last_mdfd_date TIMESTAMP(0) FORMAT 'YYYY-MM-DDbHH:MI:SS',\n  user_prfr_lang_cd_txt VARCHAR(100),\n  biz_entity_type_txt VARCHAR(100),\n  trade_rgstrtn_id VARCHAR(100),\n  reg_mchn_group_id DECIMAL(38,0),\n  dayphone_cntry_cd DECIMAL(4,0),\n  address_count DECIMAL(9,0),\n  paybox_country_id VARCHAR(100),\n  paybox_number VARCHAR(100),\n  cbt_sbscr_dt DATE FORMAT 'YYYY-MM-DD',\n  hash_initl_real_email VARBYTE(32)\n)\nPRIMARY INDEX TPIdw_users_info_n_w(user_regn_id);\n";
        HiveTableDefinition ctd = new HiveTableDefinition(command);
        String ddl = TdDdlBuilder.build(ctd);
        System.out.println(ddl);
        Assert.assertEquals(dmlStr, ddl);
    }
}