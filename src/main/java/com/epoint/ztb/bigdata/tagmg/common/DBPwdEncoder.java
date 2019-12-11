package com.epoint.ztb.bigdata.tagmg.common;

import com.epoint.ztb.bigdata.tagmg.constants.SysConstant;

public class DBPwdEncoder
{

    public static String encode(String pwd) {
        return SMUtils.SM4EncryptByECB(pwd, SMUtils.genKey(SysConstant.PWD_KEY));
    }

    public static String decode(String cipher) {
        return SMUtils.SM4DecryptByECB(cipher, SMUtils.genKey(SysConstant.PWD_KEY));
    }
}
