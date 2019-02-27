package org.apache.gdr.ext.util;

import org.apache.gdr.common.exception.GdrRuntimeException;
import junit.framework.Assert;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHiveSqlParser {
    @Test
    public void testSimpleDDL() throws SemanticException, ParseException {
        String command = "create table t (c1 int, c2 string, c3 decimal(18,0), c4 varchar(100))";
        HiveTableDefinition ctd = new HiveTableDefinition(command);

        Assert.assertEquals("4 columns", 4, ctd.getCols().size());
        Assert.assertTrue(ctd.validateCols());
    }

    @Test
    public void testHiveDDL1() throws SemanticException, ParseException {
        String command = "CREATE EXTERNAL TABLE dw_users_info_n_w(user_regn_id decimal(6,0), user_id decimal(18,0), user_slctd_id string, user_sts_code decimal(3,0), user_confirm_code string, last_modfd timestamp, userid_last_chg timestamp, user_flags decimal(16,0), email string, hash_email binary, user_cntry_id decimal(10,0), uvdetail decimal(16,0), uvrating decimal(5,0), feedback_score decimal(18,0), user_site_id decimal(3,0), cobrand_prtnr_id decimal(9,0), blng_curncy_id decimal(3,0), equifax_sts string, user_sex_flag decimal(18,0), top_slr_lvl_code decimal(3,0), flagsex2 decimal(18,0), motors_seller_level decimal(8,0), mbyte_userid string, flagsex3 decimal(18,0), flagsex4 int, flagsex5 int, flagsex6 int, geo_pstl_code string, bbp_min_alwd_fdbk_score int, ebx_elgbl_slr_yn_id tinyint, ebx_qlfd_slr_yn_id tinyint, slr_ebx_optn_status_id tinyint, ebx_cs_frc_out_yn_id tinyint, ebx_cs_frc_in_yn_id tinyint, ebx_regstrd_user_yn_id tinyint, sdc_sys_user_yn_id tinyint, regstrd_slng_chnl_id tinyint, flagsex7 int, guest_ind string, pp_auto_linking_disabled_ind string, flagsex8 int, flagsex9 int, flagsex10 int, flagsex11 int, acct_type_cd decimal(4,0), flagsex12 decimal(18,0), addr1 string, addr2 string, flags01 int, addr2_used_yn_id tinyint, banner_prtnr_id decimal(9,0), cc_onfile_yn string, city string, comp string, dayphone string, equifax_attempts decimal(4,0), equifax_last_modfd_date timestamp, faxphone string, gender_mfu string, good_crd_yn string, nightphone string, pref_categ_interest1_id decimal(18,0), pref_categ_interest2_id decimal(18,0), pref_categ_interest3_id decimal(18,0), pref_categ_interest4_id decimal(18,0), pstl_code string, state string, top_slr_initiate_date timestamp, user_cre_date timestamp, user_cre_prd_id string, user_cre_week_id string, user_name string, user_ip_addr string, req_email_count decimal(10,0), last_modified_user_info timestamp, payment_type decimal(9,0), date_confirm timestamp, date_of_birth timestamp, site_personal_id string, eop_verify_stat decimal(4,0), eop_last_verify timestamp, last_banner_prtnr_id decimal(9,0), aol_master_id decimal(18,0), tax_id_application_date timestamp, tax_id_confirm_date timestamp, tax_status decimal(3,0), motors_seller_initiated_date date, linked_paypal_acct string, paypal_link_state decimal(2,0), verification_method decimal(3,0), verification_type_code decimal(8,0), verification_date date, cellphone string, anonymous_email_yn_flag_id tinyint, reg_test_grp_id smallint, busn_type_id tinyint, user_dsgntn_id tinyint, user_dsgntn_dt date, user_dsgntn_tm timestamp, ebx_pref_last_modfd_dt date, ebx_pref_last_modfd_tm timestamp, pri_user_id decimal(18,0), top_byr_gmb_score int, smsphone string, slr_pp_dflt_email string, slr_reg_ip_addr string, user_first_name string, user_last_name string, reg_initd_site_id decimal(4,0), reg_cmpltd_site_id decimal(4,0), syi_block_begin_dt date, syi_block_dt date, slr_block_rsn_type decimal(4,0), cs_force_bsns_dt date, cs_bsns_exmptn_dt date, exclude_ship_to_loc_modify_ts timestamp, hash_addr1 binary, hash_addr2 binary, hash_dayphone binary, hash_faxphone binary, hash_nightphone binary, hash_user_name binary, hash_user_ip_addr binary, hash_cellphone binary, hash_smsphone binary, hash_slr_pp_dflt_email binary, hash_user_first_name binary, hash_user_last_name binary, payment_type_last_mdfd_date timestamp, user_prfr_lang_cd_txt string, biz_entity_type_txt string, trade_rgstrtn_id string, reg_mchn_group_id decimal(38,0), dayphone_cntry_cd decimal(4,0), address_count decimal(9,0), paybox_country_id string, paybox_number string, cbt_sbscr_dt date, hash_initl_real_email binary)\n"
                + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n"
                + "WITH SERDEPROPERTIES (\n"
                + "  'line.delim' = '\n',\n"
                + "  'field.delim' = '\007'\n"
                + ")\n"
                + "STORED AS\n"
                + "  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n"
                + "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n"
                + "LOCATION 'hdfs:///data/dw_users_info_n_w'\n";
        HiveTableDefinition ctd = new HiveTableDefinition(command);

        assertEquals("139 columns", 139, ctd.getCols().size());
        assertTrue(ctd.validateCols());
        assertEquals("\007", ctd.getFieldDelim());
        assertEquals("\n", ctd.getLineDelim());
        assertEquals("org.apache.hadoop.mapred.TextInputFormat", ctd.getInputFormat());
        assertEquals("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", ctd.getOutputFormat());
        assertEquals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", ctd.getSerde());
    }

    @Test
    public void testHiveDDL2() throws SemanticException, ParseException {
        String command = "CREATE EXTERNAL TABLE table1(f STRING)\n"
                + " COMMENT 'table 1'\n"
                + " ROW FORMAT DELIMITED\n"
                + " FIELDS TERMINATED BY ';'\n"
                + " STORED AS TEXTFILE\n"
                + " LOCATION '/path/to/hdfs/'\n"
                + " tblproperties ('skip.header.line.count'='1')\n";
        HiveTableDefinition ctd = new HiveTableDefinition(command);

        assertEquals("1 columns", 1, ctd.getCols().size());
        assertTrue(ctd.validateCols());
        assertEquals(";", ctd.getFieldDelim());
        assertNull(ctd.getLineDelim());
        assertEquals("org.apache.hadoop.mapred.TextInputFormat", ctd.getInputFormat());
        assertEquals("org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat", ctd.getOutputFormat());
        assertEquals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", ctd.getSerde());
    }

    @Test
    public void testDupCol() throws SemanticException, ParseException {
        String command = "create table t (c1 int, c2 string, c3 decimal(18,0), c1 varchar(100))";
        HiveTableDefinition ctd = new HiveTableDefinition(command);

        Assert.assertEquals("4 columns", 4, ctd.getCols().size());
        Assert.assertFalse(ctd.validateCols());
    }

    @SuppressWarnings("unused")
    @Test(expected = GdrRuntimeException.class)
    public void testCreateAs() throws SemanticException, ParseException {
        String command = "create table t as select a,b,c from t0";
        HiveTableDefinition ctd = new HiveTableDefinition(command);
    }

    @SuppressWarnings("unused")
    @Test(expected = GdrRuntimeException.class)
    public void testCreateLike() throws SemanticException, ParseException {
        String command = "create table t like t0";
        HiveTableDefinition ctd = new HiveTableDefinition(command);
    }
}
