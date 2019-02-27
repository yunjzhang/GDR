package org.apache.gdr.common;

import org.apache.gdr.common.util.AbUtils;
import org.apache.gdr.common.util.CommentType;
import junit.framework.Assert;
import org.junit.Test;

public class AbUtilsTest {

    @Test
    public void TestSqlComment() {
        String sql = "/*\n" +
                "This is a test for multiple lines comments\n" +
                "*/\n" +
                "select\n" +
                "item_id,--primark key\n" +
                "auct_end_dt, /* end date*/\n" +
                "   --te--xt-- as text,--hello\n" +
                "'  --te' as name,\n" +
                "      '-- te' as second_name,\n" +
                "'sfdaj--ff' as third_name, -- it’s third_name\n" +
                "--'test' as forth_name,\n" +
                "'----asdfasdf---' as fifth_name, 'asdfasdf','--6name--' as sisth_name, ----'this is comment''df''\n" +
                "/* name /*\n" +
                "   --adc\n" +
                "   fdasljfa\n" +
                "   */\n" +
                "semiColon_in_comment/* there is a ;\n" +
                "in comment */\n" +
                "--'asv' as bbb,\n" +
                "c1, --/*\n" +
                "+/*test,\n" +
                "+*/ c2 \n" +
                "/*c3\n" +
                "--c4*/\n" +
                "from dw_lstg_item;";
        String outStr = "\nselect\n" +
                "item_id,\n" +
                "auct_end_dt, \n" +
                "   \n" +
                "'  --te' as name,\n" +
                "      '-- te' as second_name,\n" +
                "'sfdaj--ff' as third_name, \n" +
                "\n" +
                "'----asdfasdf---' as fifth_name, 'asdfasdf','--6name--' as sisth_name, \n" +
                "\n" +
                "semiColon_in_comment\n\n" +
                "c1, \n" +
                "+ c2 \n\n" +
                "from dw_lstg_item;";
        String c = AbUtils.removeComment(sql, CommentType.SQL);
        Assert.assertEquals(outStr, c);
    }

    @Test
    public void TestDmlComment() {
        String dml = "utf8 record" +
                "\n  datetime(\"YYYY-MM-DD HH24:MI:SS\")(\"\007\") cmpgn_run_date = NULL(\"\") /*TIMESTAMP(0)*/;" +
                "\n  string(\"\007\") src_trckng_cd = NULL(\"\") /*VARCHAR(64) CHARACTER SET LATIN*/;" +
                "\n  decimal(\"\007\",0) user_id = NULL(\"\") /*DECIMAL(18)*/;" +
                "\n  string(\"\007\") guid = NULL(\"\") /*VARCHAR(32) CHARACTER SET LATIN*/;" +
                "\n  decimal(\"\007\") session_skey = NULL(\"\") /*BIGINT*/;" +
                "\n  decimal(\"\007\") seqnum = NULL(\"\") /*INTEGER*/;" +
                "\n  datetime(\"YYYY-MM-DD HH24:MI:SS\")(\"\007\") event_ts = NULL(\"\") /*TIMESTAMP(0)*/;" +
                "\n  decimal(\"\007\") open_click_id = NULL(\"\") /*BYTEINT*/;" +
                "\n  string(\"\007\") brwsr_type_txt = NULL(\"\") /*VARCHAR(50) CHARACTER SET LATIN*/;" +
                "\n  string(\"\007\") brwsr_vrsn_txt = NULL(\"\") ;//VARCHAR(50) CHARACTER SET LATIN" +
                "\n  string(\"\n\") os_device_type_txt = NULL(\"\") ; /VARCHAR(50) CHARACTER SET LATIN" +
                "\nend";
        String outStr = "utf8 record" +
                "\n  datetime(\"YYYY-MM-DD HH24:MI:SS\")(\"\007\") cmpgn_run_date = NULL(\"\") ;" +
                "\n  string(\"\007\") src_trckng_cd = NULL(\"\") ;" +
                "\n  decimal(\"\007\",0) user_id = NULL(\"\") ;" +
                "\n  string(\"\007\") guid = NULL(\"\") ;" +
                "\n  decimal(\"\007\") session_skey = NULL(\"\") ;" +
                "\n  decimal(\"\007\") seqnum = NULL(\"\") ;" +
                "\n  datetime(\"YYYY-MM-DD HH24:MI:SS\")(\"\007\") event_ts = NULL(\"\") ;" +
                "\n  decimal(\"\007\") open_click_id = NULL(\"\") ;" +
                "\n  string(\"\007\") brwsr_type_txt = NULL(\"\") ;" +
                "\n  string(\"\007\") brwsr_vrsn_txt = NULL(\"\") ;" +
                "\n  string(\"\n\") os_device_type_txt = NULL(\"\") ; /VARCHAR(50) CHARACTER SET LATIN" +
                "\nend";
        String c = AbUtils.removeComment(dml);
        Assert.assertEquals(outStr, c);
    }

    @Test
    public void TestSql2() {
        String sql = "/***************************************************************************************\n" +
                "# Title         :\n" +
                "# Filename      :\n" +
                "#\n" +
                "# Date          Ver#    Modified By(Name)       Change and Reason for Change\n" +
                "# --------      ----    -----------------       -----------------------------------------\n" +
                "# 2018/03/30    1.0     Keith Sun               spark version\n" +
                "***************************************************************************************/\n" +
                "\n" +
                "/*-----------------------------------------------------------------\n" +
                "below code is translated from dw_ctlg.ctlg_prod_fact_w.new_ins.sql\n" +
                "-----------------------------------------*/------------------------- */ create xxxx\n" +
                "\n" +
                "--V1.8 change ${workingDB}.CTLG_PROD_FACT_W to volatile table CTLG_PROD_FACT_W\n" +
                "create or replace temporary view  ctlg_prod_fact_w as select * from table b;\n" +
                "\n" +
                "\n" +
                "/*fdaf\n" +
                "--ddafa*/\n" +
                "\n" +
                "select * from table user_info;\n" +
                "\n" +
                "\n" +
                "select\n" +
                "c1, --/*\n" +
                "/*test,\n" +
                "*/ c2 from table c;\n" +
                "\n" +
                "select\n" +
                "c1\n" +
                "--test c2\n" +
                "-- /*comment*/\n" +
                "from table c;";
        String outputStr = "\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "create or replace temporary view  ctlg_prod_fact_w as select * from table b;\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "select * from table user_info;\n" +
                "\n" +
                "\n" +
                "select\n" +
                "c1, \n" +
                " c2 from table c;\n" +
                "\n" +
                "select\n" +
                "c1\n" +
                "\n" +
                "\n" +
                "from table c;";
        String c = AbUtils.removeComment(sql, CommentType.SQL);
        Assert.assertEquals(outputStr, c);
    }

    @Test
    public void TestSql3() {
        String sql = "/*\n" +
                "This is a test for multiple lines comments\n" +
                "*/\n" +
                "select\n" +
                "item_id,--primark key\n" +
                "auct_end_dt, /* end date*/\n" +
                "   --te--xt-- as text,--hello\n" +
                "'  --te' as name,\n" +
                "      '-- te' as second_name,\n" +
                "'sfdaj--ff' as third_name, -- it’s third_name\n" +
                "--'test' as forth_name,\n" +
                "'----asdfasdf---' as fifth_name, 'asdfasdf','--6name--' as sisth_name, ----'this is comment''df''\n" +
                "/* name /*\n" +
                "   --adc\n" +
                "   fdasljfa\n" +
                "   */\n" +
                "semiColon_in_comment/* there is a ;\n" +
                "in comment */\n" +
                "from dw_lstg_item;\n";
        String outputStr = "\n" +
                "select\n" +
                "item_id,\n" +
                "auct_end_dt, \n" +
                "   \n" +
                "'  --te' as name,\n" +
                "      '-- te' as second_name,\n" +
                "'sfdaj--ff' as third_name, \n" +
                "\n" +
                "'----asdfasdf---' as fifth_name, 'asdfasdf','--6name--' as sisth_name, \n" +
                "\n" +
                "semiColon_in_comment\n" +
                "from dw_lstg_item;\n";
        String c = AbUtils.removeComment(sql, CommentType.SQL);
        Assert.assertEquals(outputStr, c);
    }
}
