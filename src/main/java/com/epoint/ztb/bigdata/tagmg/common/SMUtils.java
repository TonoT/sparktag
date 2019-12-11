package com.epoint.ztb.bigdata.tagmg.common;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.bouncycastle.crypto.digests.SM3Digest;
import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.util.encoders.Hex;

import com.google.common.base.Charsets;

public class SMUtils
{

    // 加解密的字节块大小
    private static final int BLOCK_SIZE = 16;

    /**
     * SM4的ECB加密算法
     * 
     * @param content
     *            待加密内容
     * @param key
     *            密钥
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String SM4EncryptByECB(String content, String key) {
        SM4Engine sm4Engine = new SM4Engine();
        byte[] in = addDigit(content.getBytes(Charsets.UTF_8));
        sm4Engine.init(true, new KeyParameter(Hex.decode(key)));
        byte[] out = new byte[in.length];
        int times = in.length / BLOCK_SIZE;
        for (int i = 0; i < times; i++) {
            sm4Engine.processBlock(in, i * BLOCK_SIZE, out, i * BLOCK_SIZE);
        }
        return Hex.toHexString(out);
    }

    /**
     * SM4的ECB解密算法
     * 
     * @param cipher
     *            密文内容
     * @param key
     *            密钥
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String SM4DecryptByECB(String cipher, String key) {
        SM4Engine sm4Engine = new SM4Engine();
        sm4Engine.init(false, new KeyParameter(Hex.decode(key)));
        byte[] in = Hex.decode(cipher);
        byte[] out = new byte[in.length];
        int times = in.length / BLOCK_SIZE;
        for (int i = 0; i < times; i++) {
            sm4Engine.processBlock(in, i * BLOCK_SIZE, out, i * BLOCK_SIZE);
        }
        return new String(removeDigit(out), Charsets.UTF_8);
    }

    /**
     * 补位
     * 
     * @param in
     * @return
     */
    public static byte[] addDigit(byte[] in) {
        int m = in.length / BLOCK_SIZE;
        byte[] in2 = new byte[(m + 1) * BLOCK_SIZE];
        for (int i = 0; i < in.length; i++) {
            in2[i] = in[i];
        }
        for (int i = in.length; i < in2.length; i++) {
            in2[i] = 0;
        }
        return in2;
    }

    /**
     * 去位
     * 
     * @param in
     * @return
     */
    public static byte[] removeDigit(byte[] in) {
        List<Byte> byts = new ArrayList<Byte>(in.length);
        boolean isadd = false;
        for (int i = in.length - 1; i >= 0; i--) {
            if (!isadd && in[i] != 0) {
                isadd = true;
            }
            if (isadd) {
                byts.add(0, in[i]);
            }
        }

        byte[] out = new byte[byts.size()];
        for (int i = 0; i < byts.size(); i++) {
            out[i] = byts.get(i);
        }
        return out;
    }

    public static String genKey(String key) {
        String sm3 = SM3(key);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < sm3.length(); i += 2) {
            sb.append(sm3.charAt(i));
        }
        return sb.toString();
    }

    /**
     * SM3计算hashCode
     * 
     * @param srcData
     *            待计算数据
     * @return
     */
    public static String SM3(String content) {
        SM3Digest sm3Digest = new SM3Digest();
        byte[] srcData = content.getBytes(Charsets.UTF_8);
        sm3Digest.update(srcData, 0, srcData.length);
        byte[] encrypt = new byte[sm3Digest.getDigestSize()];
        sm3Digest.doFinal(encrypt, 0);
        return Hex.toHexString(encrypt);
    }
}
